from __future__ import annotations

import os
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape

import pandas as pd
import requests
from django.conf import settings
from django.db import close_old_connections

from core.models import TareaCatalogacion, SellerVtex

logger: logging.Logger = logging.getLogger(__name__)

CANTIDAD_WORKERS = 100
TAMANIO_PAGINA = 200
REINTENTOS_MAXIMOS = 3
ESPERA_ENTRE_REINTENTOS = 2  # segundos, se multiplica por numero de intento

# Las 52 columnas del export oficial + extras utiles
COLUMNAS_EXPORT = [
    'EAN', 'ACTIVO', 'FOTO', 'CATALOGADO',
    '_IDSKU', '_NombreSku', '_ActivarSKUSiEsPosible', '_SkuActivo', '_EANSKU',
    '_Altura', '_AlturaReal', '_Anchura', '_AnchuraReal',
    '_Longitud', '_LongitudReal', '_Peso', '_PesoReal',
    '_UnidadMedida', '_MultiplicadorUnidad', '_CodigoReferenciaSKU',
    '_ValorFidelidad', '_FechaEstimadaLlegada', '_CodigoFabricante',
    '_IDProducto', '_NombreProducto', '_DescripcionCortaProducto',
    '_ProductoActivo', '_CodigoReferenciaProducto', '_MostrarEnSitio',
    '_LinkTexto', '_DescripcionProducto', '_FechaLanzamientoProducto',
    '_PalabrasClave', '_TituloSitio', '_DescripcionMetaTag',
    '_IDProveedor', '_MostrarSinStock', '_Kit',
    '_IDDepartamento', '_NombreDepartamento', '_IDCategoria', '_NombreCategoria',
    '_IDMarca', '_Marca', '_PesoVolumetrico', '_CondicionComercial',
    '_Tiendas', '_Accesorios', '_Similares', '_Sugerencias',
    '_ShowTogether', '_Adjunto',
    # Extras utiles fuera del export oficial
    'Motivo', 'Precio', 'Stock',
]


@dataclass
class _ContextoVtex:
    """Agrupa datos de conexion a VTEX (seller, marketplace, sesiones HTTP)."""
    seller: SellerVtex
    headers_seller: dict
    url_base_seller: str
    headers_marketplace: dict
    url_base_marketplace: str
    sales_channels_filtro: list[int]
    session_marketplace: requests.Session = field(default_factory=requests.Session)
    session_seller: requests.Session = field(default_factory=requests.Session)


class ExportCatalogoService:

    # Cada cuantos items se guarda el progreso en la BD (reduce I/O de disco)
    INTERVALO_PROGRESO = 200

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache_categorias: dict[str, dict] = {}
        self._cache_marcas: dict[str, dict] = {}
        self._cache_productos: dict[str, dict | None] = {}
        self._cache_lock = threading.Lock()
        self._progreso_pendiente: int = 0

    # -- Metodo principal -----------------------------------------------------

    def ejecutar(
        self, tarea: TareaCatalogacion, seller_id: int,
        sales_channels_filtro: list[int] | None = None,
        incluir_precio_stock: bool = True,
    ) -> None:
        """Exporta el catalogo completo de un seller con las 52 columnas del export VTEX.

        Pipeline por fases:
        1. Obtener detalles de todos los SKUs
        2. Obtener productos, categorias y marcas unicos
        3. (Opcional) Obtener precio y stock de cada SKU
        4. Construir filas y generar Excel
        """
        contexto = self._inicializar(tarea, seller_id, sales_channels_filtro)
        if contexto is None:
            return

        try:
            # Fase 0: Obtener todos los SKU IDs
            self._log(tarea, "Fase 0: Obteniendo listado de SKU IDs...")
            sku_ids = self._obtener_todos_los_sku_ids(tarea, contexto)
            if not sku_ids:
                self._log(tarea, "No se encontraron SKU IDs para este seller.")
                self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)
                return

            total_skus = len(sku_ids)
            self._log(tarea, f"SKU IDs encontrados: {total_skus}")

            # Fase 1: Fetch todos los SKU details
            self._log(tarea, f"Fase 1/{4 if incluir_precio_stock else 2}: Obteniendo detalles de {total_skus} SKUs ({CANTIDAD_WORKERS} workers)...")
            tarea.progreso_total = total_skus
            tarea.progreso_actual = 0
            tarea.save(update_fields=['progreso_total', 'progreso_actual'])

            detalles_skus = self._fase_fetch_skus(tarea, sku_ids, contexto)

            # Extraer IDs unicos de productos, categorias y marcas
            ids_productos = set()
            ids_categorias = set()
            ids_marcas = set()
            for ds in detalles_skus:
                if ds is None:
                    continue
                pid = ds.get('ProductId')
                if pid:
                    ids_productos.add(str(pid))
                cid = ds.get('CategoryId')
                if cid:
                    ids_categorias.add(str(cid))
                bid = ds.get('BrandId')
                if bid and not ds.get('BrandName'):
                    ids_marcas.add(str(bid))

            # Fase 2: Fetch productos unicos + categorias + marcas
            total_consultas = len(ids_productos) + len(ids_categorias) + len(ids_marcas)
            self._log(tarea, f"Fase 2/{4 if incluir_precio_stock else 2}: {len(ids_productos)} productos, {len(ids_categorias)} categorias, {len(ids_marcas)} marcas unicos ({total_consultas} calls)...")
            tarea.progreso_total = total_consultas
            tarea.progreso_actual = 0
            tarea.save(update_fields=['progreso_total', 'progreso_actual'])

            self._fase_fetch_lookups(tarea, ids_productos, ids_categorias, ids_marcas, contexto)

            # Extraer IDs de departamentos de productos cacheados
            ids_departamentos = set()
            for pid in ids_productos:
                prod = self._cache_productos.get(pid)
                if prod:
                    did = prod.get('DepartmentId')
                    if did:
                        ids_departamentos.add(str(did))
            # Fetch departamentos que no esten ya cacheados como categorias
            departamentos_nuevos = ids_departamentos - set(self._cache_categorias.keys())
            if departamentos_nuevos:
                self._log(tarea, f"  + {len(departamentos_nuevos)} departamentos adicionales...")
                self._fase_fetch_batch(
                    tarea, list(departamentos_nuevos),
                    lambda did: self._obtener_categoria(did, contexto)
                )

            # Fase 3 (opcional): Pricing + Stock
            precios: dict[int, float | None] = {}
            stocks: dict[int, int | None] = {}
            if incluir_precio_stock:
                self._log(tarea, f"Fase 3/4: Obteniendo precio y stock de {total_skus} SKUs ({CANTIDAD_WORKERS} workers)...")
                tarea.progreso_total = total_skus * 2
                tarea.progreso_actual = 0
                tarea.save(update_fields=['progreso_total', 'progreso_actual'])
                precios, stocks = self._fase_fetch_precio_stock(tarea, sku_ids, contexto)
            else:
                self._log(tarea, "Precio/Stock omitido (no solicitado).")

            # Fase 4: Construir resultados
            fase_num = 4 if incluir_precio_stock else 3
            self._log(tarea, f"Fase {fase_num}: Construyendo {total_skus} filas...")
            resultados = self._construir_resultados(
                sku_ids, detalles_skus, precios, stocks, contexto, incluir_precio_stock
            )

            # Generar Excel
            self._generar_excel(tarea, resultados)
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)
            self._log(tarea, f"Export finalizado. {total_skus} SKUs procesados.")
        finally:
            contexto.session_marketplace.close()
            contexto.session_seller.close()

    # -- Inicializacion de contexto -------------------------------------------

    def _inicializar(
        self, tarea: TareaCatalogacion, seller_id: int, sales_channels_filtro: list[int] | None
    ) -> _ContextoVtex | None:
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
        headers_marketplace = {
            **headers_base,
            'X-VTEX-API-AppKey': marketplace.app_key,
            'X-VTEX-API-AppToken': marketplace.app_token,
        }
        headers_seller = {
            **headers_base,
            'X-VTEX-API-AppKey': seller.app_key,
            'X-VTEX-API-AppToken': seller.app_token,
        }

        session_marketplace = requests.Session()
        session_marketplace.headers.update(headers_marketplace)
        session_marketplace.mount('https://', requests.adapters.HTTPAdapter(
            pool_connections=CANTIDAD_WORKERS, pool_maxsize=CANTIDAD_WORKERS + 10
        ))

        session_seller = requests.Session()
        session_seller.headers.update(headers_seller)
        session_seller.mount('https://', requests.adapters.HTTPAdapter(
            pool_connections=CANTIDAD_WORKERS, pool_maxsize=CANTIDAD_WORKERS + 10
        ))

        return _ContextoVtex(
            seller=seller,
            headers_seller=headers_seller,
            url_base_seller=f"https://{seller.account_name}.vtexcommercestable.com.br",
            headers_marketplace=headers_marketplace,
            url_base_marketplace=f"https://{marketplace.account_name}.vtexcommercestable.com.br",
            sales_channels_filtro=sales_channels_filtro if sales_channels_filtro else [1, 3],
            session_marketplace=session_marketplace,
            session_seller=session_seller,
        )

    # -- Obtener todos los SKU IDs (paginado) ---------------------------------

    def _obtener_todos_los_sku_ids(self, tarea: TareaCatalogacion, contexto: _ContextoVtex) -> list[int]:
        todos_los_ids: list[int] = []
        page = 1
        while True:
            url = (
                f"{contexto.url_base_marketplace}"
                f"/api/catalog_system/pvt/sku/stockkeepingunitids"
                f"?page={page}&pagesize={TAMANIO_PAGINA}"
            )
            datos = self._request_con_retry(url, contexto.session_marketplace)
            if datos is None:
                self._log(tarea, f"Error obteniendo pagina {page} de SKU IDs. Abortando.")
                self._actualizar_estado(tarea, TareaCatalogacion.Estado.ERROR)
                return []
            if not datos:
                break
            todos_los_ids.extend(datos)
            self._log(tarea, f"Pagina {page}: {len(datos)} SKU IDs (total: {len(todos_los_ids)})")
            if len(datos) < TAMANIO_PAGINA:
                break
            page += 1
        return todos_los_ids

    # -- Pipeline: Fases de obtencion de datos --------------------------------

    def _fase_fetch_skus(
        self, tarea: TareaCatalogacion, sku_ids: list[int], contexto: _ContextoVtex
    ) -> list[dict | None]:
        """Fase 1: obtiene detalle de cada SKU via stockkeepingunitbyid."""
        detalles_skus: list[dict | None] = [None] * len(sku_ids)

        def _obtener_detalle_sku(idx_skuid: tuple[int, int]) -> tuple[int, dict | None]:
            idx, sku_id = idx_skuid
            url = f"{contexto.url_base_marketplace}/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"
            return idx, self._request_con_retry(url, contexto.session_marketplace, silenciar_404=True)

        with ThreadPoolExecutor(max_workers=CANTIDAD_WORKERS) as pool:
            futuros = {
                pool.submit(_obtener_detalle_sku, (i, sid)): i
                for i, sid in enumerate(sku_ids)
            }
            for futuro in as_completed(futuros):
                try:
                    idx, datos = futuro.result()
                    detalles_skus[idx] = datos
                except Exception as e:
                    idx = futuros[futuro]
                    logger.error(f"Error fetch SKU idx {idx}: {e}")
                self._incrementar_progreso(tarea)

        self._flush_progreso(tarea)
        return detalles_skus

    def _fase_fetch_lookups(
        self, tarea: TareaCatalogacion,
        product_ids: set[str], category_ids: set[str], brand_ids: set[str],
        contexto: _ContextoVtex
    ) -> None:
        """Fase 2: obtiene productos, categorias y marcas unicos en un solo pool."""

        def _fetch_producto(pid: str):
            self._obtener_producto(pid, contexto)

        def _fetch_categoria(cid: str):
            self._obtener_categoria(cid, contexto)

        def _fetch_marca(bid: str):
            self._obtener_marca(bid, contexto)

        # Armar lista de consultas heterogeneas (productos + categorias + marcas)
        tareas: list[tuple] = []
        for pid in product_ids:
            tareas.append((_fetch_producto, pid))
        for cid in category_ids:
            tareas.append((_fetch_categoria, cid))
        for bid in brand_ids:
            tareas.append((_fetch_marca, bid))

        with ThreadPoolExecutor(max_workers=CANTIDAD_WORKERS) as pool:
            futuros = [pool.submit(fn, arg) for fn, arg in tareas]
            for f in as_completed(futuros):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"Error en lookup: {e}")
                self._incrementar_progreso(tarea)
        self._flush_progreso(tarea)

    def _fase_fetch_batch(self, tarea: TareaCatalogacion, ids: list[str], fn) -> None:
        """Obtiene datos en batch para IDs adicionales."""
        with ThreadPoolExecutor(max_workers=CANTIDAD_WORKERS) as pool:
            futuros = [pool.submit(fn, id_) for id_ in ids]
            for f in as_completed(futuros):
                try:
                    f.result()
                except Exception:
                    pass

    def _fase_fetch_precio_stock(
        self, tarea: TareaCatalogacion, sku_ids: list[int], contexto: _ContextoVtex
    ) -> tuple[dict[int, float | None], dict[int, int | None]]:
        """Fase 3: obtiene precio y stock de todos los SKUs."""
        precios: dict[int, float | None] = {}
        stocks: dict[int, int | None] = {}
        precio_lock = threading.Lock()
        stock_lock = threading.Lock()

        def _fetch_precio(sku_id: int):
            resultado = self._obtener_precio(sku_id, contexto)
            with precio_lock:
                precios[sku_id] = resultado

        def _fetch_stock(sku_id: int):
            resultado = self._obtener_stock(sku_id, contexto)
            with stock_lock:
                stocks[sku_id] = resultado

        # Intercalar precio y stock para distribuir la carga entre ambos endpoints
        tareas: list[tuple] = []
        for sid in sku_ids:
            tareas.append((_fetch_precio, sid))
            tareas.append((_fetch_stock, sid))

        with ThreadPoolExecutor(max_workers=CANTIDAD_WORKERS) as pool:
            futuros = [pool.submit(fn, arg) for fn, arg in tareas]
            for f in as_completed(futuros):
                try:
                    f.result()
                except Exception:
                    pass
                self._incrementar_progreso(tarea)

        self._flush_progreso(tarea)
        return precios, stocks

    # -- Construir resultados -------------------------------------------------

    def _construir_resultados(
        self, sku_ids: list[int], detalles_skus: list[dict | None],
        precios: dict[int, float | None], stocks: dict[int, int | None],
        contexto: _ContextoVtex, incluir_precio_stock: bool = True,
    ) -> list[dict]:
        """Fase final: combina datos de cache para armar las 52+ columnas por SKU."""
        resultados: list[dict] = []

        for i, sku_id in enumerate(sku_ids):
            datos_sku = detalles_skus[i]
            if datos_sku is None:
                resultados.append(self._resultado_error(sku_id, 'Error al consultar catalogo'))
                continue

            product_id = str(datos_sku.get('ProductId', '') or '')
            ean = datos_sku.get('AlternateIds', {}).get('Ean', '') or datos_sku.get('Ean', '') or ''
            category_id = str(datos_sku.get('CategoryId', '') or '')
            brand_id = str(datos_sku.get('BrandId', '') or '')

            # Producto (de cache)
            datos_prod = self._cache_productos.get(product_id) if product_id else None

            # Categoria (de cache)
            nombre_categoria = ''
            if category_id:
                nombre_categoria = self._cache_categorias.get(category_id, {}).get('Name', '')
            elif datos_prod and datos_prod.get('CategoryId'):
                category_id = str(datos_prod['CategoryId'])
                nombre_categoria = self._cache_categorias.get(category_id, {}).get('Name', '')

            # Departamento (de cache)
            department_id = str((datos_prod or {}).get('DepartmentId', '') or '')
            nombre_departamento = ''
            if department_id:
                nombre_departamento = self._cache_categorias.get(department_id, {}).get('Name', '')

            # Marca (de cache o de datos_sku)
            nombre_marca = datos_sku.get('BrandName', '') or ''
            if not nombre_marca and brand_id:
                nombre_marca = self._cache_marcas.get(brand_id, {}).get('Name', '')

            # Precio y stock
            precio = precios.get(sku_id)
            stock = stocks.get(sku_id)

            descripcion = (datos_prod or {}).get('Description', '') or ''

            # Calcular columnas
            foto = bool(datos_sku.get('Images', []))
            calidad_ok = self._calcular_calidad(datos_sku, descripcion)
            activo, motivo = self._calcular_activo(
                datos_sku, datos_prod, foto, calidad_ok,
                contexto.sales_channels_filtro, precio, stock,
                incluir_precio_stock,
            )
            catalogado = activo and foto and bool(str(ean).strip())

            sc_list = datos_sku.get('SalesChannels', [])
            sc_str = ', '.join(str(sc) for sc in sc_list) if sc_list else ''
            dimension = datos_sku.get('Dimension', {}) if isinstance(datos_sku.get('Dimension'), dict) else {}

            resultados.append({
                'EAN': str(ean),
                'ACTIVO': 'SI' if activo else 'NO',
                'FOTO': 'SI' if foto else 'NO',
                'CATALOGADO': 'SI' if catalogado else 'NO',
                '_IDSKU': str(sku_id),
                '_NombreSku': datos_sku.get('NameComplete', '') or datos_sku.get('SkuName', ''),
                '_ActivarSKUSiEsPosible': _si_no(datos_sku.get('ActivateIfPossible')),
                '_SkuActivo': _si_no(datos_sku.get('IsActive')),
                '_EANSKU': str(ean),
                '_Altura': _num(datos_sku.get('Height') or dimension.get('height')),
                '_AlturaReal': _num(datos_sku.get('RealHeight') or dimension.get('realHeight')),
                '_Anchura': _num(datos_sku.get('Width') or dimension.get('width')),
                '_AnchuraReal': _num(datos_sku.get('RealWidth') or dimension.get('realWidth')),
                '_Longitud': _num(datos_sku.get('Length') or dimension.get('length')),
                '_LongitudReal': _num(datos_sku.get('RealLength') or dimension.get('realLength')),
                '_Peso': _num(datos_sku.get('Weight') or dimension.get('weight')),
                '_PesoReal': _num(datos_sku.get('RealWeight') or dimension.get('realWeight')),
                '_UnidadMedida': str(datos_sku.get('MeasurementUnit', '') or ''),
                '_MultiplicadorUnidad': _num(datos_sku.get('UnitMultiplier')),
                '_CodigoReferenciaSKU': str(datos_sku.get('RefId', '') or ''),
                '_ValorFidelidad': _num(datos_sku.get('RewardValue')),
                '_FechaEstimadaLlegada': str(datos_sku.get('EstimatedDateArrival', '') or ''),
                '_CodigoFabricante': str(datos_sku.get('ManufacturerCode', '') or ''),
                '_IDProducto': str(product_id),
                '_NombreProducto': datos_sku.get('ProductName', ''),
                '_DescripcionCortaProducto': (datos_prod or {}).get('ShortDescription', '') or '',
                '_ProductoActivo': _si_no(datos_sku.get('IsProductActive')),
                '_CodigoReferenciaProducto': str(
                    datos_sku.get('ProductRefId', '') or (datos_prod or {}).get('RefId', '') or ''
                ),
                '_MostrarEnSitio': _si_no((datos_prod or {}).get('IsVisible')),
                '_LinkTexto': str((datos_prod or {}).get('LinkId', '') or ''),
                '_DescripcionProducto': descripcion,
                '_FechaLanzamientoProducto': str((datos_prod or {}).get('ReleaseDate', '') or ''),
                '_PalabrasClave': str((datos_prod or {}).get('KeyWords', '') or ''),
                '_TituloSitio': str((datos_prod or {}).get('Title', '') or ''),
                '_DescripcionMetaTag': str((datos_prod or {}).get('MetaTagDescription', '') or ''),
                '_IDProveedor': str((datos_prod or {}).get('SupplierId', '') or ''),
                '_MostrarSinStock': _si_no((datos_prod or {}).get('ShowWithoutStock')),
                '_Kit': _si_no(datos_sku.get('IsKit')),
                '_IDDepartamento': str(department_id),
                '_NombreDepartamento': nombre_departamento,
                '_IDCategoria': str(category_id),
                '_NombreCategoria': nombre_categoria,
                '_IDMarca': str(brand_id),
                '_Marca': nombre_marca,
                '_PesoVolumetrico': _num(dimension.get('cubicweight') or dimension.get('CubicWeight')),
                '_CondicionComercial': str(datos_sku.get('CommercialConditionId', '') or ''),
                '_Tiendas': sc_str,
                '_Accesorios': '',
                '_Similares': '',
                '_Sugerencias': '',
                '_ShowTogether': '',
                '_Adjunto': '',
                'Motivo': motivo if not activo else '',
                'Precio': precio,
                'Stock': stock,
            })

        return resultados

    # -- Logica de columnas ---------------------------------------------------

    def _calcular_calidad(self, datos_sku: dict, descripcion: str) -> bool:
        product_name = datos_sku.get('ProductName', '')
        tiene_foto = bool(datos_sku.get('Images', []))
        nombre_ok = not product_name.isupper() if product_name else False
        foto_ok = tiene_foto
        descripcion_ok = not descripcion.isupper() if descripcion else True
        categorias = datos_sku.get('ProductCategories', {})
        categorias_invalidas = {'deshabilitados', 'categoria default', 'categoría default'}
        categoria_ok = True
        for cat_name in categorias.values():
            if str(cat_name).strip().lower() in categorias_invalidas:
                categoria_ok = False
                break
        return nombre_ok and foto_ok and descripcion_ok and categoria_ok

    def _calcular_activo(
        self, datos_sku: dict, datos_prod: dict | None, foto: bool, calidad_ok: bool,
        sales_channels_filtro: list[int], precio: float | None, stock: int | None,
        incluir_precio_stock: bool = True,
    ) -> tuple[bool, str]:
        motivos: list[str] = []
        if not foto:
            motivos.append('Sin imagenes')
        if not calidad_ok:
            product_name = datos_sku.get('ProductName', '')
            if product_name and product_name.isupper():
                motivos.append('Nombre todo mayusculas')
            categorias = datos_sku.get('ProductCategories', {})
            categorias_invalidas = {'deshabilitados', 'categoria default', 'categoría default'}
            for cat_name in categorias.values():
                if str(cat_name).strip().lower() in categorias_invalidas:
                    motivos.append(f'Categoria: {cat_name}')
                    break
            if not motivos:
                motivos.append('No catalogado (calidad)')
        if not datos_sku.get('IsActive', False):
            motivos.append('SKU inactivo')
        if not datos_sku.get('IsProductActive', False):
            motivos.append('Producto inactivo')
        if datos_prod and not datos_prod.get('IsVisible', False):
            motivos.append('No visible en sitio')
        elif datos_prod is None:
            motivos.append('Sin datos de producto')
        if datos_prod and not datos_prod.get('ShowWithoutStock', False):
            motivos.append('ShowWithoutStock desactivado')
        sc_list = datos_sku.get('SalesChannels', [])
        sc_ids = set(sc_list) if sc_list else set()
        if not any(sc in sc_ids for sc in sales_channels_filtro):
            canales_str = ', '.join(str(sc) for sc in sales_channels_filtro)
            motivos.append(f'Sin ninguno de SC [{canales_str}]')
        # Precio y stock solo se verifican si fueron solicitados
        if incluir_precio_stock:
            if precio is None:
                motivos.append('Sin precio')
            if stock is not None and stock <= 0:
                motivos.append('Sin stock')
        activo = len(motivos) == 0
        return activo, ', '.join(motivos)

    # -- Consultas con cache --------------------------------------------------

    def _obtener_producto(self, product_id: str, contexto: _ContextoVtex) -> dict | None:
        with self._cache_lock:
            if product_id in self._cache_productos:
                return self._cache_productos[product_id]
        url = f"{contexto.url_base_marketplace}/api/catalog/pvt/product/{product_id}"
        datos = self._request_con_retry(url, contexto.session_marketplace, silenciar_404=True)
        with self._cache_lock:
            self._cache_productos[product_id] = datos
        return datos

    def _obtener_categoria(self, category_id: str, contexto: _ContextoVtex) -> dict:
        with self._cache_lock:
            if category_id in self._cache_categorias:
                return self._cache_categorias[category_id]
        url = f"{contexto.url_base_marketplace}/api/catalog/pvt/category/{category_id}"
        datos = self._request_con_retry(url, contexto.session_marketplace, silenciar_404=True)
        resultado = datos if datos else {'Name': '', 'Id': category_id}
        with self._cache_lock:
            self._cache_categorias[category_id] = resultado
        return resultado

    def _obtener_marca(self, brand_id: str, contexto: _ContextoVtex) -> dict:
        with self._cache_lock:
            if brand_id in self._cache_marcas:
                return self._cache_marcas[brand_id]
        url = f"{contexto.url_base_marketplace}/api/catalog_system/pvt/brand/{brand_id}"
        datos = self._request_con_retry(url, contexto.session_marketplace, silenciar_404=True)
        resultado = datos if datos else {'Name': '', 'Id': brand_id}
        with self._cache_lock:
            self._cache_marcas[brand_id] = resultado
        return resultado

    # -- Precio y stock -------------------------------------------------------

    def _obtener_precio(self, sku_id: int, contexto: _ContextoVtex) -> float | None:
        url = f"{contexto.url_base_seller}/api/pricing/prices/{sku_id}"
        datos = self._request_con_retry(url, contexto.session_seller, silenciar_404=True)
        if datos:
            return datos.get('basePrice')
        return None

    def _obtener_stock(self, sku_id: int, contexto: _ContextoVtex) -> int | None:
        url = f"{contexto.url_base_seller}/api/logistics/pvt/inventory/skus/{sku_id}"
        datos = self._request_con_retry(url, contexto.session_seller, silenciar_404=True)
        if datos:
            balance = datos.get('balance', [])
            return sum(
                max(a.get('totalQuantity', 0) - a.get('reservedQuantity', 0), 0)
                for a in balance
            )
        return None

    # -- HTTP con reintentos --------------------------------------------------

    def _request_con_retry(
        self, url: str, session: requests.Session, silenciar_404: bool = False
    ) -> dict | list | None:
        for intento in range(1, REINTENTOS_MAXIMOS + 1):
            try:
                resp = session.get(url=url, timeout=30)
                if resp.status_code == 429:
                    wait = ESPERA_ENTRE_REINTENTOS * intento
                    logger.warning(f"429 en {url}, esperando {wait}s ({intento}/{REINTENTOS_MAXIMOS})")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404 and silenciar_404:
                    return None
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if intento < REINTENTOS_MAXIMOS:
                    time.sleep(ESPERA_ENTRE_REINTENTOS * intento)
                    continue
                logger.error(f"HTTP error {url}: {e}")
                return None
            except Exception as e:
                logger.error(f"Error {url}: {e}")
                return None
        return None

    # -- Utilidades -----------------------------------------------------------

    def _resultado_error(self, sku_id: int, motivo: str) -> dict:
        resultado = {col: '' for col in COLUMNAS_EXPORT}
        resultado.update({
            'ACTIVO': 'ERROR', 'FOTO': 'ERROR', 'CATALOGADO': 'ERROR',
            '_IDSKU': str(sku_id), 'Motivo': motivo, 'Precio': None, 'Stock': None,
        })
        return resultado

    def _generar_excel(self, tarea: TareaCatalogacion, resultados: list[dict]) -> None:
        ahora = datetime.now()
        nombre = f'EXPORT_VTEX_{ahora.day}_{ahora.month}.xlsx'
        directorio_relativo = os.path.join('output', str(ahora.year), str(ahora.month), str(ahora.day))
        directorio = os.path.join(settings.MEDIA_ROOT, directorio_relativo)
        os.makedirs(directorio, exist_ok=True)
        ruta_final = os.path.join(directorio, nombre)

        df = pd.DataFrame(resultados, columns=COLUMNAS_EXPORT)
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            df[col] = df[col].apply(lambda v: _limpiar_para_excel(v) if isinstance(v, str) else v)
        df.to_excel(ruta_final, index=False)

        tarea.archivo_resultado = os.path.join(directorio_relativo, nombre)
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
            self._progreso_pendiente += 1
            if self._progreso_pendiente >= self.INTERVALO_PROGRESO:
                incremento = self._progreso_pendiente
                self._progreso_pendiente = 0
                tarea.refresh_from_db(fields=['progreso_actual'])
                tarea.progreso_actual += incremento
                tarea.save(update_fields=['progreso_actual'])

    def _flush_progreso(self, tarea: TareaCatalogacion) -> None:
        """Guarda el progreso pendiente que no alcanzo el intervalo."""
        with self._lock:
            if self._progreso_pendiente > 0:
                incremento = self._progreso_pendiente
                self._progreso_pendiente = 0
                tarea.refresh_from_db(fields=['progreso_actual'])
                tarea.progreso_actual += incremento
                tarea.save(update_fields=['progreso_actual'])


# -- Funciones auxiliares de formato ------------------------------------------

def _si_no(valor) -> str:
    if valor is None:
        return ''
    if isinstance(valor, bool):
        return 'SI' if valor else 'NO'
    if isinstance(valor, str):
        return 'SI' if valor.lower() in ('true', 'sí', 'si', 'yes') else 'NO'
    return 'SI' if valor else 'NO'


def _num(valor) -> str:
    if valor is None:
        return ''
    return str(valor)


_ILLEGAL_XML_CHARS = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')
_HTML_TAG = re.compile(r'<[^>]+>')


def _limpiar_para_excel(valor: str) -> str:
    if not valor:
        return valor
    texto = unescape(valor)
    texto = _HTML_TAG.sub(' ', texto)
    texto = _ILLEGAL_XML_CHARS.sub('', texto)
    texto = ' '.join(texto.split())
    return texto.strip()
