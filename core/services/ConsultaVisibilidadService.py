from __future__ import annotations

import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import requests
from django.conf import settings
from django.db import close_old_connections

from core.models import (
    TareaCatalogacion, SellerVtex, SkuVtex, ProductoVtex, ConsultaVisibilidad
)

logger: logging.Logger = logging.getLogger(__name__)

MAX_WORKERS = 5


@dataclass
class _ContextoVtex:
    """Agrupa los datos de conexion a VTEX para no repetirlos en cada metodo."""
    seller: SellerVtex
    headers_seller: dict
    url_base_seller: str
    headers_marketplace: dict
    url_base_marketplace: str


class ConsultaVisibilidadService:

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ── Metodos publicos ───────────────────────────────────────────────

    def ejecutar(self, tarea: TareaCatalogacion, sku_ids: list[str], seller_id: int) -> None:
        """Consulta la visibilidad de cada SKU en el seller indicado."""
        contexto = self._inicializar(tarea, seller_id)
        if contexto is None:
            return

        self._log(tarea, f"SKUs a consultar: {len(sku_ids)}")

        elementos = [{'sku_id': sid} for sid in sku_ids]
        resultados = self._procesar_concurrente(tarea, elementos, contexto)

        self._generar_excel(tarea, resultados)
        self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)
        self._log(tarea, f"Consulta finalizada. {len(resultados)} SKUs procesados.")

    def ejecutar_por_ean(self, tarea: TareaCatalogacion, eans: list[str], seller_id: int) -> None:
        """Consulta la visibilidad de SKUs a partir de EANs."""
        contexto = self._inicializar(tarea, seller_id)
        if contexto is None:
            return

        self._log(tarea, f"EANs a consultar: {len(eans)}")

        elementos = [{'ean': ean} for ean in eans]
        resultados = self._procesar_concurrente(tarea, elementos, contexto)

        self._generar_excel(tarea, resultados)
        self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)
        self._log(tarea, f"Consulta finalizada. {len(resultados)} EANs procesados.")

    # ── Inicializacion ─────────────────────────────────────────────────

    def _inicializar(self, tarea: TareaCatalogacion, seller_id: int) -> _ContextoVtex | None:
        """Configura seller, marketplace, headers y URLs. Retorna None si falla."""
        self._actualizar_estado(tarea, TareaCatalogacion.Estado.PROCESANDO)

        try:
            seller = SellerVtex.objects.get(id=seller_id)
        except SellerVtex.DoesNotExist:
            self._log(tarea, f"Error: No se encontro el seller con ID {seller_id}")
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.ERROR)
            return None

        marketplace = seller.marketplace if seller.marketplace else seller
        self._log(tarea, f"Seller: {seller.nombre} ({seller.account_name})")
        if marketplace != seller:
            self._log(tarea, f"Marketplace madre: {marketplace.nombre} ({marketplace.account_name})")

        headers_base = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        return _ContextoVtex(
            seller=seller,
            headers_seller={
                **headers_base,
                'X-VTEX-API-AppKey': seller.app_key,
                'X-VTEX-API-AppToken': seller.app_token,
            },
            url_base_seller=f"https://{seller.account_name}.vtexcommercestable.com.br",
            headers_marketplace={
                **headers_base,
                'X-VTEX-API-AppKey': marketplace.app_key,
                'X-VTEX-API-AppToken': marketplace.app_token,
            },
            url_base_marketplace=f"https://{marketplace.account_name}.vtexcommercestable.com.br",
        )

    # ── Procesamiento concurrente ──────────────────────────────────────

    def _procesar_concurrente(
        self, tarea: TareaCatalogacion, elementos: list[dict], contexto: _ContextoVtex
    ) -> list[dict]:
        """Procesa elementos en paralelo usando ThreadPoolExecutor."""
        # Lista pre-alocada para mantener el orden original
        resultados: list[dict | None] = [None] * len(elementos)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futuro_a_indice = {
                executor.submit(self._procesar_elemento, tarea, elemento, contexto): i
                for i, elemento in enumerate(elementos)
            }

            for futuro in as_completed(futuro_a_indice):
                indice = futuro_a_indice[futuro]
                try:
                    resultados[indice] = futuro.result()
                except Exception as e:
                    elemento = elementos[indice]
                    etiqueta = elemento.get('ean') or elemento.get('sku_id', '?')
                    logger.error(f"Error procesando {etiqueta}: {e}", exc_info=True)
                    resultados[indice] = {
                        **elemento,
                        'visible': False,
                        'motivo': 'Error inesperado',
                        'stock': None,
                        'precio': None,
                        'tiene_imagenes': None,
                    }
                finally:
                    self._incrementar_progreso(tarea)

        return resultados  # type: ignore[return-value]

    def _procesar_elemento(self, tarea: TareaCatalogacion, elemento: dict, contexto: _ContextoVtex) -> dict:
        """Procesa un elemento (SKU ID directo o EAN que hay que resolver primero)."""
        # Asegurar conexion DB fresca en cada thread
        close_old_connections()

        ean = elemento.get('ean')
        sku_id = elemento.get('sku_id')

        if ean and not sku_id:
            sku_id = self._resolver_ean(ean, contexto)
            if not sku_id:
                self._log(tarea, f"EAN {ean}: NO ENCONTRADO")
                return {
                    'ean': ean, 'sku_id': '', 'visible': False,
                    'motivo': 'EAN no encontrado',
                    'stock': None, 'precio': None, 'tiene_imagenes': None,
                }
            self._log(tarea, f"EAN {ean} -> SKU {sku_id}")

        resultado = self._consultar_visibilidad_sku(tarea, sku_id, contexto)

        if ean:
            resultado['ean'] = ean

        etiqueta = f"EAN {ean} (SKU {sku_id})" if ean else f"SKU {sku_id}"
        estado_texto = "VISIBLE" if resultado['visible'] else f"NO VISIBLE ({resultado['motivo']})"
        self._log(tarea, f"{etiqueta}: {estado_texto}")

        return resultado

    # ── Resolucion EAN -> SKU ID ───────────────────────────────────────

    def _resolver_ean(self, ean: str, contexto: _ContextoVtex) -> str | None:
        """Resuelve un EAN a SKU ID via la API de VTEX. Retorna None si no se encuentra."""
        try:
            respuesta = requests.get(
                url=contexto.url_base_marketplace + "/api/catalog_system/pvt/sku/stockkeepingunitbyean/" + str(ean),
                headers=contexto.headers_marketplace,
            )
            respuesta.raise_for_status()
            datos = respuesta.json()
            sku_id = str(datos.get("Id", ""))
            return sku_id if sku_id else None
        except Exception as e:
            logger.error(f"Error resolviendo EAN {ean}: {e}")
            return None

    # ── Logica de visibilidad (unica, compartida) ──────────────────────

    def _consultar_visibilidad_sku(
        self, tarea: TareaCatalogacion, sku_id: str, contexto: _ContextoVtex
    ) -> dict:
        """Consulta visibilidad de un SKU: catalogo, precio y stock."""
        visible = True
        motivo = ""
        precio = None
        stock = None
        tiene_imagenes = None

        producto, _ = ProductoVtex.objects.get_or_create(productId=sku_id)
        sku, _ = SkuVtex.objects.get_or_create(skuId=sku_id, defaults={'producto': producto})

        # 1. Catalogo (imagenes, SKU activo, producto activo)
        try:
            respuesta = requests.get(
                url=contexto.url_base_marketplace + "/api/catalog_system/pvt/sku/stockkeepingunitbyid/" + sku_id,
                headers=contexto.headers_marketplace,
            )
            respuesta.raise_for_status()
        except Exception as e:
            logger.error(f"Error en request a VTEX API (catalogo) SKU {sku_id}: {e}")
            return {
                'sku_id': sku_id, 'visible': False, 'motivo': 'Error al consultar catalogo',
                'stock': None, 'precio': None, 'tiene_imagenes': None,
            }

        datos_catalogo = respuesta.json()

        # Actualizar ProductoVtex con el ID real
        product_id_real = str(datos_catalogo.get("ProductId", ""))
        if product_id_real and producto.productId != product_id_real:
            producto_real, _ = ProductoVtex.objects.get_or_create(productId=product_id_real)
            if sku.producto != producto_real:
                sku.producto = producto_real
                sku.save(update_fields=['producto'])
            if not producto.skus.exists() and producto.productId != product_id_real:
                producto.delete()

        lista_imagenes = datos_catalogo.get("Images", [])
        sku_activo = datos_catalogo.get("IsActive", False)
        producto_activo = datos_catalogo.get("IsProductActive", False)
        tiene_imagenes = bool(lista_imagenes)

        if not lista_imagenes:
            visible = False
            motivo = "Sin imagenes"
            logger.info(f"La respuesta del endpoint fue: {datos_catalogo}")
        elif not sku_activo:
            visible = False
            motivo = "SKU no activo"
        elif not producto_activo:
            visible = False
            motivo = "Producto no activo"

        # 2. Precio
        if visible:
            try:
                respuesta = requests.get(
                    url=contexto.url_base_seller + "/api/pricing/prices/" + sku_id,
                    headers=contexto.headers_seller,
                )
                respuesta.raise_for_status()
                datos_precio = respuesta.json()
                precio = datos_precio.get("basePrice", 0)
                if not precio:
                    visible = False
                    motivo = "Sin precio"
            except Exception as e:
                logger.error(f"Error en request a VTEX API (precio) SKU {sku_id}: {e}")
                visible = False
                motivo = "Sin precio (error al consultar)"

        # 3. Stock
        if visible:
            try:
                respuesta = requests.get(
                    url=contexto.url_base_seller + "/api/logistics/pvt/inventory/skus/" + sku_id,
                    headers=contexto.headers_seller,
                )
                respuesta.raise_for_status()
                datos_stock = respuesta.json()
                lista_almacenes = datos_stock.get("balance", [])

                tiene_stock = any(
                    almacen.get("hasUnlimitedQuantity", False)
                    or almacen.get("totalQuantity", 0) > almacen.get("reservedQuantity", 0)
                    for almacen in lista_almacenes
                )
                stock = sum(
                    max(almacen.get("totalQuantity", 0) - almacen.get("reservedQuantity", 0), 0)
                    for almacen in lista_almacenes
                )
                if not tiene_stock:
                    visible = False
                    motivo = "Sin stock"
            except Exception as e:
                logger.error(f"Error en request a VTEX API (stock) SKU {sku_id}: {e}")
                visible = False
                motivo = "Sin stock (error al consultar)"

        # Guardar en DB
        ConsultaVisibilidad.objects.create(
            sku=sku,
            seller=contexto.seller,
            tarea=tarea,
            visible=visible,
            motivo=motivo,
            stock=stock,
            precio=precio,
            tiene_imagenes=tiene_imagenes,
        )

        return {
            'sku_id': sku_id,
            'visible': visible,
            'motivo': motivo,
            'stock': stock,
            'precio': precio,
            'tiene_imagenes': tiene_imagenes,
        }

    # ── Utilidades ─────────────────────────────────────────────────────

    def _generar_excel(self, tarea: TareaCatalogacion, resultados: list[dict]) -> None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre = f'visibilidad_{timestamp}.xlsx'
        ruta_final = os.path.join(settings.MEDIA_ROOT, 'catalogacion', nombre)
        os.makedirs(os.path.dirname(ruta_final), exist_ok=True)

        df = pd.DataFrame(resultados)
        df.to_excel(ruta_final, index=False)

        tarea.archivo_resultado = f'catalogacion/{nombre}'
        tarea.save(update_fields=['archivo_resultado'])
        self._log(tarea, f"Excel generado: {nombre}")

    def _actualizar_estado(self, tarea: TareaCatalogacion, estado: str) -> None:
        tarea.estado = estado
        tarea.save(update_fields=['estado'])

    def _log(self, tarea: TareaCatalogacion, mensaje: str) -> None:
        with self._lock:
            tarea.refresh_from_db(fields=['logs'])
            tarea.agregar_log(mensaje)

    def _incrementar_progreso(self, tarea: TareaCatalogacion) -> None:
        with self._lock:
            tarea.refresh_from_db(fields=['progreso_actual'])
            tarea.progreso_actual += 1
            tarea.save(update_fields=['progreso_actual'])
