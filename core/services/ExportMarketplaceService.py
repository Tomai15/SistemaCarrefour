from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape

import requests
from django.conf import settings
from numpy.matlib import empty
from openpyxl import Workbook

from core.models import TareaCatalogacion, SellerVtex, SellerExterno

logger: logging.Logger = logging.getLogger(__name__)

CANTIDAD_WORKERS = 50
REINTENTOS_MAXIMOS = 3
ESPERA_ENTRE_REINTENTOS = 2

COLUMNAS_EXPORT = [
    'isActive', 'IDSKU', 'NombreSku', 'EANSKU', 'CodigoReferenciaSKU',
    'IDProducto', 'NombreDepartamento', 'IDCategoria', 'NombreCategoria',
    'IDMarca', 'Marca', 'CondicionComercial',
    'PrecioLista', 'PrecioBase', 'Stock',
    'URLProducto', 'URLImg', 'NivelesCategoria', 'Cucardas', 'Ribbons',
]


@dataclass
class _ContextoMarketplace:
    seller_vtex: SellerVtex
    headers: dict
    url_base: str
    session: requests.Session = field(default_factory=requests.Session)


class ExportMarketplaceService:

    INTERVALO_PROGRESO = 200

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._progreso_pendiente: int = 0
        self._estado: str = ''
        self._logs: str = ''
        self._progreso_actual: int = 0
        self._progreso_total: int = 0
        self._ruta_temporal: str = ''
        # Cache thread-safe para categorias, marcas, productos y skus
        self._cache_categorias: dict[str, dict] = {}
        self._cache_marcas: dict[str, str] = {}
        self._cache_productos: dict[str, dict] = {}
        self._cache_specs: dict[str, list] = {}
        self._cache_skus: dict[str, dict] = {}
        self._condiciones_comerciales: dict[int, str] = {}
        self._cache_lock = threading.Lock()

    # -- Metodo principal -----------------------------------------------------

    def ejecutar(
        self, tarea: TareaCatalogacion,
        seller_ids: list[str] | None = None,
    ) -> None:
        self._iniciar_estado_temporal(tarea)
        contexto = None
        try:
            contexto = self._inicializar(tarea)
            if contexto is None:
                return

            self._cargar_condiciones_comerciales(tarea, contexto)

            if seller_ids:
                sellers = list(SellerExterno.objects.filter(seller_id__in=seller_ids))
                nombres = ', '.join(s.nombre for s in sellers)
                self._log(tarea, f"Sellers seleccionados: {nombres}")
            else:
                sellers = list(SellerExterno.objects.filter(is_active=True))
                self._log(tarea, f"Exportando TODOS los sellers externos activos ({len(sellers)})")

            if not sellers:
                self._log(tarea, "No hay sellers externos para procesar.")
                self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)
                return

            inicio_total = time.time()
            todas_las_filas: list[dict] = []
            self._set_progreso(len(sellers), 0)

            for i, seller_ext in enumerate(sellers):
                self._log(tarea, f"[{i+1}/{len(sellers)}] Procesando seller: {seller_ext.nombre} ({seller_ext.seller_id})")
                filas_basicas = self._obtener_offers_seller(tarea, contexto, seller_ext)
                self._log(tarea, f"  -> {len(filas_basicas)} SKUs obtenidos de offers")

                if filas_basicas:
                    self._log(tarea, f"  Enriqueciendo datos (categorias, marcas, specs, URLs)...")
                    self._enriquecer_filas(tarea, contexto, filas_basicas)
                    todas_las_filas.extend(filas_basicas)

                self._set_progreso(len(sellers), i + 1)

            self._log(tarea, f"Generando Excel con {len(todas_las_filas)} filas totales...")
            self._generar_excel(tarea, todas_las_filas)
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.COMPLETADO)

            tiempo_total = time.time() - inicio_total
            self._log(tarea, f"Export finalizado: {len(todas_las_filas)} SKUs de {len(sellers)} sellers ({tiempo_total:.0f}s)")
        except Exception as e:
            logger.error(f"Error en ExportMarketplaceService: {e}", exc_info=True)
            self._log(tarea, f"Error: {e}")
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.ERROR)
        finally:
            self._finalizar_estado(tarea)
            if contexto:
                contexto.session.close()

    # -- Sincronizar sellers --------------------------------------------------

    @staticmethod
    def sincronizar_sellers(seller_vtex: SellerVtex) -> tuple[int, int]:
        marketplace = seller_vtex.marketplace if seller_vtex.marketplace else seller_vtex
        url_base = f"https://{marketplace.account_name}.vtexcommercestable.com.br"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-VTEX-API-AppKey': marketplace.app_key,
            'X-VTEX-API-AppToken': marketplace.app_token,
        }

        nuevos = 0
        actualizados = 0
        page_from = 0
        page_size = 100

        while True:
            url = f"{url_base}/api/seller-register/pvt/sellers?from={page_from}&to={page_from + page_size}&isActive=true&integration=vtex-seller"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Error sincronizando sellers: {e}")
                break

            items = data.get('items', [])
            if not items:
                break

            for item in items:
                sid = str(item.get('id', '')).strip()
                nombre = str(item.get('name', '') or item.get('id', '')).strip()
                is_active = item.get('isActive', False)

                if not sid or not is_active:
                    continue

                obj, created = SellerExterno.objects.update_or_create(
                    seller_id=sid,
                    defaults={'nombre': nombre, 'is_active': is_active},
                )
                if created:
                    nuevos += 1
                else:
                    actualizados += 1

            paging = data.get('paging', {})
            total = paging.get('total', 0)
            if page_from + page_size >= total:
                break
            page_from += page_size

        return nuevos, actualizados

    # -- Inicializacion -------------------------------------------------------

    def _inicializar(self, tarea: TareaCatalogacion) -> _ContextoMarketplace | None:
        self._actualizar_estado(tarea, TareaCatalogacion.Estado.PROCESANDO)

        marketplace = SellerVtex.objects.filter(
            marketplace__isnull=True
        ).exclude(account_name__icontains='poc').first()

        if not marketplace:
            self._log(tarea, "Error: No se encontro un marketplace de produccion configurado.")
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.ERROR)
            return None

        self._log(tarea, f"Marketplace: {marketplace.nombre} ({marketplace.account_name})")

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-VTEX-API-AppKey': marketplace.app_key,
            'X-VTEX-API-AppToken': marketplace.app_token,
        }

        session = requests.Session()
        session.headers.update(headers)
        session.mount('https://', requests.adapters.HTTPAdapter(
            pool_connections=CANTIDAD_WORKERS, pool_maxsize=CANTIDAD_WORKERS + 10
        ))

        return _ContextoMarketplace(
            seller_vtex=marketplace,
            headers=headers,
            url_base=f"https://{marketplace.account_name}.vtexcommercestable.com.br",
            session=session,
        )

    # -- Fase 1: Obtener offers basicas de un seller --------------------------

    def _obtener_offers_seller(
        self, tarea: TareaCatalogacion, contexto: _ContextoMarketplace,
        seller_ext: SellerExterno,
    ) -> list[dict]:
        filas: list[dict] = []
        start = 0
        rows = 50

        while True:
            url = (
                f"{contexto.url_base}/api/offer-manager/pvt/offers"
                f"?fq=sellerId:{seller_ext.seller_id}&rows={rows}&start={start}"
            )
            datos = self._request_con_retry(url, contexto.session)
            if datos is None:
                self._log(tarea, f"  Error obteniendo offers en start={start} para {seller_ext.seller_id}")
                break

            if not datos:
                break

            for offer in datos:
                product_id = str(offer.get('productId', '') or '')
                category_id = str(offer.get('categoryId', '') or '')
                brand_id = str(offer.get('brandId', '') or '')

                skus = offer.get('skus', [])
                for sku in skus:
                    sku_id = str(sku.get('skuId', '') or '')
                    sku_name = sku.get('nameComplete', '') or sku.get('name', '')
                    ean = sku.get('eanId', '') or ''
                    ref_id = sku.get('refId', '') or ''
                    is_active = sku.get('isActive', False)

                    # Imagen principal
                    main_img = sku.get('mainImage') or {}
                    img_url = main_img.get('imagePath', '') or ''

                    # Precio y stock del seller especifico
                    precio_lista = None
                    precio_base = None
                    stock_total = None
                    offers_list = sku.get('offers', [])
                    for off in offers_list:
                        if str(off.get('sellerId', '')) == str(seller_ext.seller_id):
                            channels = off.get('offersPerSalesChannel', [])
                            if channels:
                                ch = channels[0]
                                precio_lista = ch.get('listPrice')
                                precio_base = ch.get('price')
                                stock_total = ch.get('availableQuantity')
                            break

                    filas.append({
                        'isActive': 'SI' if is_active else 'NO',
                        'IDSKU': sku_id,
                        'NombreSku': sku_name,
                        'EANSKU': int(ean) if str(ean).isdigit() else ean,
                        'CodigoReferenciaSKU': ref_id,
                        'IDProducto': product_id,
                        'IDCategoria': category_id,
                        'IDMarca': brand_id,
                        'PrecioLista': precio_lista / 100 if precio_lista is not None else None,
                        'PrecioBase': precio_base / 100 if precio_base is not None else None,
                        'Stock': stock_total,
                        'URLImg': img_url,
                        # Campos que se enriquecen despues
                        'NombreDepartamento': '',
                        'NombreCategoria': '',
                        'Marca': '',
                        'CondicionComercial': '',
                        'URLProducto': '',
                        'NivelesCategoria': '',
                        'Cucardas': '',
                        'Ribbons': '',
                    })

            if len(datos) < rows:
                break
            start += rows

        return filas

    # -- Fase 2: Enriquecer con APIs de catalogo ------------------------------

    def _enriquecer_filas(
        self, tarea: TareaCatalogacion, contexto: _ContextoMarketplace,
        filas: list[dict],
    ) -> None:
        # Recopilar IDs unicos
        category_ids = set()
        brand_ids = set()
        product_ids = set()
        sku_ids = set()
        for f in filas:
            if f['IDCategoria']:
                category_ids.add(f['IDCategoria'])
            if f['IDMarca']:
                brand_ids.add(f['IDMarca'])
            if f['IDProducto']:
                product_ids.add(f['IDProducto'])
            if f['IDSKU']:
                sku_ids.add(f['IDSKU'])

        # Fetch categorias en paralelo
        self._log(tarea, f"    Obteniendo {len(category_ids)} categorias...")
        self._fetch_categorias_paralelo(contexto, category_ids)

        # Fetch marcas en paralelo
        self._log(tarea, f"    Obteniendo {len(brand_ids)} marcas...")
        self._fetch_marcas_paralelo(contexto, brand_ids)

        # Fetch productos (para URL) y specs (cucardas/ribbons)
        self._log(tarea, f"    Obteniendo {len(product_ids)} productos y specifications...")
        self._fetch_productos_y_specs_paralelo(contexto, product_ids)

        # Fetch SKUs (para condicion comercial)
        self._log(tarea, f"    Obteniendo {len(sku_ids)} SKUs (condicion comercial)...")
        self._fetch_skus_paralelo(contexto, sku_ids)

        # Aplicar datos enriquecidos a cada fila
        for f in filas:
            cat_id = f['IDCategoria']
            if cat_id and cat_id in self._cache_categorias:
                cat_data = self._cache_categorias[cat_id]
                f['NombreCategoria'] = cat_data.get('nombre', '')
                f['NombreDepartamento'] = cat_data.get('departamento', '')
                f['NivelesCategoria'] = cat_data.get('niveles', '')

            brand_id = f['IDMarca']
            if brand_id and brand_id in self._cache_marcas:
                f['Marca'] = self._cache_marcas[brand_id]

            prod_id = f['IDProducto']
            if prod_id and prod_id in self._cache_productos:
                prod_data = self._cache_productos[prod_id]
                link_id = prod_data.get('LinkId', '') or ''
                if link_id:
                    f['URLProducto'] = f"https://{contexto.seller_vtex.account_name}.com.br/{link_id}/p"

            sku_id = f['IDSKU']
            if sku_id and sku_id in self._cache_skus:
                sku_data = self._cache_skus[sku_id]
                cond_id = sku_data.get('CommercialConditionId')
                if cond_id is not None:
                    f['CondicionComercial'] = self._condiciones_comerciales.get(cond_id, str(cond_id))

            if prod_id and prod_id in self._cache_specs:
                specs = self._cache_specs[prod_id]
                nombres_ribbon = (
                    'tarjeta carrefour bsf', 'mi carrefour corajudo',
                    'tarjeta mi carrefour crédito', 'mi carrefour clásico',
                    'tarjeta mi carrefour prepaga', 'pack envases',
                    'ahora 12', 'combinalo',
                )
                for spec in specs:
                    name_lower = str(spec.get('Name', '')).lower()
                    values = spec.get('Value', [])
                    val = ', '.join(str(v) for v in values) if isinstance(values, list) else str(values)
                    if 'cucarda' in name_lower:
                        f['Cucardas'] = val
                    elif any(nombre in name_lower for nombre in nombres_ribbon):
                        if f['Ribbons']:
                            f['Ribbons'] += ' | ' + val
                        else:
                            f['Ribbons'] = val

    def _fetch_categorias_paralelo(self, contexto: _ContextoMarketplace, category_ids: set[str]) -> None:
        ids_a_buscar = [cid for cid in category_ids if cid not in self._cache_categorias]
        if not ids_a_buscar:
            return

        def _fetch_cat(cat_id: str) -> tuple[str, dict]:
            # Obtener la categoria y subir hasta el departamento
            niveles = []
            current_id = cat_id
            nombre_categoria = ''
            nombre_departamento = ''

            while current_id:
                url = f"{contexto.url_base}/api/catalog/pvt/category/{current_id}"
                datos = self._request_con_retry(url, contexto.session, silenciar_404=True)
                if not datos:
                    break
                nombre = datos.get('Name', '') or ''
                niveles.append(nombre)
                if not nombre_categoria:
                    nombre_categoria = nombre
                nombre_departamento = nombre  # El ultimo siempre es el departamento
                parent_id = datos.get('FatherCategoryId')
                if parent_id and str(parent_id) != current_id:
                    current_id = str(parent_id)
                else:
                    break

            niveles.reverse()
            return cat_id, {
                'nombre': nombre_categoria,
                'departamento': nombre_departamento,
                'niveles': ' | '.join(niveles),
            }

        with ThreadPoolExecutor(max_workers=min(CANTIDAD_WORKERS, len(ids_a_buscar))) as pool:
            futuros = [pool.submit(_fetch_cat, cid) for cid in ids_a_buscar]
            for f in as_completed(futuros):
                try:
                    cat_id, datos = f.result()
                    with self._cache_lock:
                        self._cache_categorias[cat_id] = datos
                except Exception as e:
                    logger.error(f"Error fetch categoria: {e}")

    def _fetch_marcas_paralelo(self, contexto: _ContextoMarketplace, brand_ids: set[str]) -> None:
        ids_a_buscar = [bid for bid in brand_ids if bid not in self._cache_marcas]
        if not ids_a_buscar:
            return

        def _fetch_brand(brand_id: str) -> tuple[str, str]:
            url = f"{contexto.url_base}/api/catalog/pvt/brand/{brand_id}"
            datos = self._request_con_retry(url, contexto.session, silenciar_404=True)
            nombre = datos.get('Name', '') if datos else ''
            return brand_id, nombre

        with ThreadPoolExecutor(max_workers=min(CANTIDAD_WORKERS, len(ids_a_buscar))) as pool:
            futuros = [pool.submit(_fetch_brand, bid) for bid in ids_a_buscar]
            for f in as_completed(futuros):
                try:
                    brand_id, nombre = f.result()
                    with self._cache_lock:
                        self._cache_marcas[brand_id] = nombre
                except Exception as e:
                    logger.error(f"Error fetch marca: {e}")

    def _fetch_productos_y_specs_paralelo(self, contexto: _ContextoMarketplace, product_ids: set[str]) -> None:
        ids_prod = [pid for pid in product_ids if pid not in self._cache_productos]
        ids_spec = [pid for pid in product_ids if pid not in self._cache_specs]
        ids_a_buscar = list(set(ids_prod) | set(ids_spec))
        if not ids_a_buscar:
            return

        def _fetch_prod(pid: str) -> None:
            # Producto (URL, CondicionComercial)
            if pid not in self._cache_productos:
                url = f"{contexto.url_base}/api/catalog/pvt/product/{pid}"
                datos = self._request_con_retry(url, contexto.session, silenciar_404=True)
                with self._cache_lock:
                    self._cache_productos[pid] = datos or {}

            # Specifications (Cucardas, Ribbons)
            if pid not in self._cache_specs:
                url = f"{contexto.url_base}/api/catalog_system/pvt/products/{pid}/specification"
                datos = self._request_con_retry(url, contexto.session, silenciar_404=True)
                with self._cache_lock:
                    self._cache_specs[pid] = datos if isinstance(datos, list) else []

        with ThreadPoolExecutor(max_workers=min(CANTIDAD_WORKERS, len(ids_a_buscar))) as pool:
            futuros = [pool.submit(_fetch_prod, pid) for pid in ids_a_buscar]
            for f in as_completed(futuros):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"Error fetch producto/specs: {e}")

    def _cargar_condiciones_comerciales(self, tarea: TareaCatalogacion, contexto: _ContextoMarketplace) -> None:
        url = f"{contexto.url_base}/api/catalog_system/pvt/commercialcondition/list"
        datos = self._request_con_retry(url, contexto.session)
        if datos and isinstance(datos, list):
            self._condiciones_comerciales = {item['Id']: item['Name'] for item in datos}
            self._log(tarea, f"Condiciones comerciales cargadas: {len(self._condiciones_comerciales)}")
        else:
            self._log(tarea, "Advertencia: No se pudieron cargar las condiciones comerciales")

    def _fetch_skus_paralelo(self, contexto: _ContextoMarketplace, sku_ids: set[str]) -> None:
        ids_a_buscar = [sid for sid in sku_ids if sid not in self._cache_skus]
        if not ids_a_buscar:
            return

        def _fetch_sku(sku_id: str) -> tuple[str, dict]:
            url = f"{contexto.url_base}/api/catalog_system/pvt/sku/stockkeepingunitbyid/{sku_id}"
            datos = self._request_con_retry(url, contexto.session, silenciar_404=True)
            return sku_id, datos or {}

        with ThreadPoolExecutor(max_workers=min(CANTIDAD_WORKERS, len(ids_a_buscar))) as pool:
            futuros = [pool.submit(_fetch_sku, sid) for sid in ids_a_buscar]
            for f in as_completed(futuros):
                try:
                    sku_id, datos = f.result()
                    with self._cache_lock:
                        self._cache_skus[sku_id] = datos
                except Exception as e:
                    logger.error(f"Error fetch sku: {e}")

    # -- HTTP con reintentos --------------------------------------------------

    def _request_con_retry(
        self, url: str, session: requests.Session, silenciar_404: bool = False,
    ) -> dict | list | None:
        for intento in range(1, REINTENTOS_MAXIMOS + 1):
            try:
                resp = session.get(url=url, timeout=30)
                if resp.status_code == 429:
                    wait = ESPERA_ENTRE_REINTENTOS * (2 ** (intento - 1)) + random.uniform(0, 1)
                    logger.warning(f"429 en {url}, esperando {wait:.1f}s ({intento}/{REINTENTOS_MAXIMOS})")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404 and silenciar_404:
                    return None
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                if intento < REINTENTOS_MAXIMOS:
                    wait = ESPERA_ENTRE_REINTENTOS * (2 ** (intento - 1)) + random.uniform(0, 1)
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP error {url}: {e}")
                return None
            except Exception as e:
                if intento < REINTENTOS_MAXIMOS:
                    wait = ESPERA_ENTRE_REINTENTOS * (2 ** (intento - 1)) + random.uniform(0, 1)
                    logger.warning(f"Error {url}: {e}, reintentando en {wait:.1f}s ({intento}/{REINTENTOS_MAXIMOS})")
                    time.sleep(wait)
                    continue
                logger.error(f"Error {url}: {e}")
                return None
        return None

    # -- Generar Excel --------------------------------------------------------

    def _generar_excel(self, tarea: TareaCatalogacion, resultados: list[dict]) -> None:
        ahora = datetime.now()
        nombre = f'EXPORT_MARKETPLACE_{ahora.day}_{ahora.month}.xlsx'
        directorio_relativo = os.path.join('output', str(ahora.year), str(ahora.month), str(ahora.day))
        directorio = os.path.join(settings.MEDIA_ROOT, directorio_relativo)
        os.makedirs(directorio, exist_ok=True)
        ruta_final = os.path.join(directorio, nombre)

        wb = Workbook(write_only=True)
        ws = wb.create_sheet()
        ws.append(COLUMNAS_EXPORT)
        for fila in resultados:
            valores = []
            for col in COLUMNAS_EXPORT:
                v = fila.get(col, '')
                if isinstance(v, str):
                    v = _limpiar_para_excel(v)
                valores.append(v)
            ws.append(valores)
        wb.save(ruta_final)

        tarea.archivo_resultado = os.path.join(directorio_relativo, nombre)
        self._log(tarea, f"Excel generado: {nombre}")

    # -- Estado temporal (mismo patron que ExportCatalogoService) -------------

    def _iniciar_estado_temporal(self, tarea: TareaCatalogacion) -> None:
        directorio = os.path.join(settings.MEDIA_ROOT, 'tmp')
        os.makedirs(directorio, exist_ok=True)
        self._ruta_temporal = os.path.join(directorio, f'tarea_{tarea.id}.json')
        self._estado = tarea.estado
        self._logs = tarea.logs or ''
        self._progreso_actual = 0
        self._progreso_total = 0

    def _escribir_estado_temporal(self) -> None:
        datos = {
            'estado': self._estado,
            'logs': self._logs,
            'progreso_actual': self._progreso_actual,
            'progreso_total': self._progreso_total,
        }
        ruta_tmp = self._ruta_temporal + '.tmp'
        try:
            with open(ruta_tmp, 'w', encoding='utf-8') as f:
                json.dump(datos, f)
            # os.replace puede fallar en Windows si otro thread tiene el archivo abierto
            for intento in range(3):
                try:
                    os.replace(ruta_tmp, self._ruta_temporal)
                    return
                except PermissionError:
                    time.sleep(0.05)
            # Fallback: escribir directo al archivo destino
            with open(self._ruta_temporal, 'w', encoding='utf-8') as f:
                json.dump(datos, f)
        except Exception:
            pass

    def _finalizar_estado(self, tarea: TareaCatalogacion) -> None:
        tarea.estado = self._estado
        tarea.logs = self._logs
        tarea.progreso_actual = self._progreso_actual
        tarea.progreso_total = self._progreso_total
        tarea.save()
        try:
            os.remove(self._ruta_temporal)
        except OSError:
            pass

    def _actualizar_estado(self, tarea: TareaCatalogacion, estado: str) -> None:
        with self._lock:
            self._estado = estado
            self._escribir_estado_temporal()

    def _log(self, tarea: TareaCatalogacion, mensaje: str) -> None:
        with self._lock:
            if self._logs:
                self._logs += f"\n{mensaje}"
            else:
                self._logs = mensaje
            self._escribir_estado_temporal()

    def _set_progreso(self, total: int, actual: int) -> None:
        with self._lock:
            self._progreso_total = total
            self._progreso_actual = actual
            self._progreso_pendiente = 0
            self._escribir_estado_temporal()

    def _incrementar_progreso(self, tarea: TareaCatalogacion, cantidad: int = 1) -> None:
        with self._lock:
            self._progreso_pendiente += cantidad
            if self._progreso_pendiente >= self.INTERVALO_PROGRESO:
                self._progreso_actual += self._progreso_pendiente
                self._progreso_pendiente = 0
                self._escribir_estado_temporal()

    def _flush_progreso(self, tarea: TareaCatalogacion) -> None:
        with self._lock:
            if self._progreso_pendiente > 0:
                self._progreso_actual += self._progreso_pendiente
                self._progreso_pendiente = 0
                self._escribir_estado_temporal()


# -- Funciones auxiliares -----------------------------------------------------

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
