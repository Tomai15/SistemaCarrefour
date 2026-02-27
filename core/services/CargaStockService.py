from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import requests
from asgiref.sync import sync_to_async
from django.conf import settings
from playwright.async_api import async_playwright, Page, Browser

from core.models import TareaCatalogacion, SellerVtex

logger: logging.Logger = logging.getLogger(__name__)

TIMEOUT_LOGIN = 2 * 60 * 1000  # 5 minutos en ms
TIMEOUT_NAVEGACION = 30 * 1000  # 30 segundos


@dataclass
class _ContextoVtex:
    seller: SellerVtex
    headers: dict
    url_base: str
    account_name: str


class CargaStockService:

    # ── Metodo publico ─────────────────────────────────────────────────

    async def ejecutar(
        self,
        tarea: TareaCatalogacion,
        items: list[dict],
        seller_id: int,
        headless: bool = False,
    ) -> None:
        await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.PROCESANDO)

        contexto = await sync_to_async(self._inicializar_contexto)(tarea, seller_id)
        if contexto is None:
            return

        # Fase 1: Resolver EANs a SKU IDs via API
        await self._log(tarea, f"Fase 1: Resolviendo {len(items)} EANs via API...")
        await self._set_progreso_total(tarea, len(items))

        resultados: list[dict] = []
        items_resueltos: list[dict] = []

        for item in items:
            ean = str(item['ean']).strip()
            stock = int(item['stock'])
            sku_id = self._resolver_ean(ean, contexto)

            if sku_id:
                await self._log(tarea, f"EAN {ean} -> SKU {sku_id}")
                items_resueltos.append({'ean': ean, 'sku_id': sku_id, 'stock': stock})
            else:
                await self._log(tarea, f"EAN {ean}: NO ENCONTRADO")
                resultados.append({
                    'ean': ean, 'sku_id': '', 'stock_deseado': stock, 'estado': 'EAN no encontrado'
                })

        if not items_resueltos:
            await self._log(tarea, "No se resolvio ningun EAN. Generando Excel de resultados.")
            await sync_to_async(self._generar_excel)(tarea, resultados)
            await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.COMPLETADO)
            return

        await self._log(tarea, f"Fase 1 completa: {len(items_resueltos)} de {len(items)} EANs resueltos.")

        # Fase 2: Abrir navegador y esperar login manual
        await self._log(tarea, "Fase 2: Abriendo navegador VTEX admin...")
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            page = await browser.new_page()

            login_ok = await self._esperar_login(tarea, page, contexto)
            if not login_ok:
                await self._log(tarea, "ERROR: Login no completado dentro del tiempo limite.")
                await browser.close()
                await sync_to_async(self._generar_excel)(tarea, resultados + [
                    {'ean': it['ean'], 'sku_id': it['sku_id'], 'stock_deseado': it['stock'], 'estado': 'Login timeout'}
                    for it in items_resueltos
                ])
                await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.ERROR)
                return

            await self._log(tarea, "Login detectado. Iniciando carga de stock...")

            # Fase 3: Cargar stock para cada SKU
            for item in items_resueltos:
                ean = item['ean']
                sku_id = item['sku_id']
                stock = item['stock']
                try:
                    ok = await self._cargar_stock_sku(tarea, page, sku_id, stock, contexto)
                    estado = 'OK' if ok else 'Error al cargar'
                except Exception as e:
                    logger.error(f"Error cargando stock SKU {sku_id}: {e}", exc_info=True)
                    estado = f'Error: {e}'

                resultados.append({
                    'ean': ean, 'sku_id': sku_id, 'stock_deseado': stock, 'estado': estado
                })
                await self._incrementar_progreso(tarea)
                await self._log(tarea, f"SKU {sku_id} (EAN {ean}): {estado}")

            await browser.close()

        # Fase 4: Generar Excel de resultados
        await self._log(tarea, "Fase 4: Generando Excel de resultados...")
        await sync_to_async(self._generar_excel)(tarea, resultados)
        await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.COMPLETADO)
        await self._log(tarea, "Carga de stock finalizada.")

    # ── Inicializacion ─────────────────────────────────────────────────

    def _inicializar_contexto(self, tarea: TareaCatalogacion, seller_id: int) -> _ContextoVtex | None:
        try:
            seller = SellerVtex.objects.get(id=seller_id)
        except SellerVtex.DoesNotExist:
            logger.error(f"No se encontro el seller con ID {seller_id}")
            tarea.agregar_log(f"Error: No se encontro el seller con ID {seller_id}")
            self._actualizar_estado(tarea, TareaCatalogacion.Estado.ERROR)
            return None

        marketplace = seller.marketplace if seller.marketplace else seller
        logger.info(f"Seller: {seller.nombre} ({seller.account_name})")
        tarea.agregar_log(f"Seller: {seller.nombre} ({seller.account_name})")

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-VTEX-API-AppKey': seller.app_key,
            'X-VTEX-API-AppToken': seller.app_token,
        }

        return _ContextoVtex(
            seller=seller,
            headers=headers,
            url_base=f"https://{seller.account_name}.vtexcommercestable.com.br",
            account_name=seller.account_name,
        )

    # ── Resolucion EAN -> SKU ID ───────────────────────────────────────

    def _resolver_ean(self, ean: str, contexto: _ContextoVtex) -> str | None:
        try:
            respuesta = requests.get(
                url=contexto.url_base + "/api/catalog_system/pvt/sku/stockkeepingunitbyean/" + str(ean),
                headers=contexto.headers,
            )
            respuesta.raise_for_status()
            datos = respuesta.json()
            sku_id = str(datos.get("Id", ""))
            return sku_id if sku_id else None
        except Exception as e:
            logger.error(f"Error resolviendo EAN {ean}: {e}")
            return None

    # ── Playwright: Login manual ───────────────────────────────────────

    async def _esperar_login(self, tarea: TareaCatalogacion, page: Page, contexto: _ContextoVtex) -> bool:
        url_admin = f"https://{contexto.account_name}.myvtex.com/admin"
        await self._log(tarea, f"Navegando a {url_admin}")
        await self._log(tarea, "Por favor, complete el login manualmente en la ventana del navegador (5 min timeout)...")

        await page.goto(url_admin, wait_until='domcontentloaded')

        dominio_admin = f"{contexto.account_name}.myvtex.com"

        # Paso 1: Esperar a que el redirect OAuth empiece (URL sale de myvtex.com)
        await self._log(tarea, "Esperando redirect de login...")
        try:
            while True:
                await asyncio.sleep(2)
                if dominio_admin not in page.url:
                    await self._log(tarea, f"Redirect detectado: {page.url[:80]}...")
                    break
        except Exception:
            pass

        # Paso 2: Esperar a que el usuario complete el login y vuelva al admin
        inicio = asyncio.get_event_loop().time()
        limite = TIMEOUT_LOGIN / 1000

        while (asyncio.get_event_loop().time() - inicio) < limite:
            try:
                url_actual = page.url
                # Verificar que volvio al dominio admin (no a /api/ ni /login)
                if dominio_admin in url_actual and '/api/' not in url_actual:
                    # Esperar a que cargue el contenido
                    await asyncio.sleep(3)
                    await self._log(tarea, f"Login completado (URL: {url_actual})")
                    return True
            except Exception:
                pass
            await asyncio.sleep(3)

        return False

    # ── Playwright: Cargar stock de un SKU ─────────────────────────────

    async def _cargar_stock_sku(
        self, tarea: TareaCatalogacion, page: Page, sku_id: str, stock: int, contexto: _ContextoVtex
    ) -> bool:
        url_inventario = f"https://{contexto.account_name}.myvtex.com/admin/app/inventory?skuId={sku_id}"
        await page.goto(url_inventario, wait_until='domcontentloaded')

        # Esperar a que cargue la tabla de inventario
        await page.wait_for_timeout(3000)
        await page.wait_for_selector('table[data-testid="vtex-table-v2"] tbody tr', timeout=TIMEOUT_NAVEGACION)

        # Buscar la fila del almacen general
        filas = page.locator('table[data-testid="vtex-table-v2"] tbody tr')
        cantidad_filas = await filas.count()

        fila_almacen = None
        for i in range(cantidad_filas):
            fila = filas.nth(i)
            texto_almacen = await fila.locator('td:nth-child(2)').inner_text()
            logger.info(f"Almacen fila {i}: {repr(texto_almacen)}")
            # Normalizar: quitar espacios, non-breaking spaces, etc
            texto_norm = texto_almacen.strip().replace('\xa0', ' ').lower()
            if 'almacen general' in texto_norm:
                fila_almacen = fila
                break

        if fila_almacen is None:
            await self._log(tarea, f"SKU {sku_id}: No se encontro 'Almacen General'. Almacenes disponibles:")
            for i in range(cantidad_filas):
                texto = await filas.nth(i).locator('td:nth-child(2)').inner_text()
                await self._log(tarea, f"  - {texto.strip()}")
            return False

        # Encontrar el input de "Actualizar recuento" (columna 8)
        input_stock = fila_almacen.locator('td:nth-child(8) input')
        await input_stock.click()
        await input_stock.fill(str(stock))

        # Esperar a que aparezca la barra de acciones flotante y hacer clic en Guardar
        await page.wait_for_timeout(500)
        boton_guardar = page.locator('div[class*="FloatingActionBar"] button:has-text("Guardar")')

        try:
            await boton_guardar.wait_for(timeout=5000)
            await boton_guardar.click()
        except Exception:
            # Fallback: buscar cualquier boton con texto Guardar/Save
            boton_guardar_alt = page.get_by_text('Guardar', exact=False).first
            await boton_guardar_alt.click()

        # Esperar a que se guarde (la barra flotante desaparece o aparece mensaje de exito)
        await page.wait_for_timeout(3000)

        return True

    # ── Utilidades ─────────────────────────────────────────────────────

    def _generar_excel(self, tarea: TareaCatalogacion, resultados: list[dict]) -> None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre = f'carga_stock_{timestamp}.xlsx'
        ruta_final = os.path.join(settings.MEDIA_ROOT, 'catalogacion', nombre)
        os.makedirs(os.path.dirname(ruta_final), exist_ok=True)

        df = pd.DataFrame(resultados)
        df.to_excel(ruta_final, index=False)

        tarea.archivo_resultado = f'catalogacion/{nombre}'
        tarea.save(update_fields=['archivo_resultado'])
        logger.info(f"Excel generado: {nombre}")
        tarea.agregar_log(f"Excel generado: {nombre}")

    def _actualizar_estado(self, tarea: TareaCatalogacion, estado: str) -> None:
        tarea.estado = estado
        tarea.save(update_fields=['estado'])

    async def _log(self, tarea: TareaCatalogacion, mensaje: str) -> None:
        logger.info(mensaje)
        await sync_to_async(tarea.agregar_log)(mensaje)

    async def _incrementar_progreso(self, tarea: TareaCatalogacion) -> None:
        def _update() -> None:
            tarea.refresh_from_db(fields=['progreso_actual'])
            tarea.progreso_actual += 1
            tarea.save(update_fields=['progreso_actual'])
        await sync_to_async(_update)()

    async def _set_progreso_total(self, tarea: TareaCatalogacion, total: int) -> None:
        def _update() -> None:
            tarea.progreso_total = total
            tarea.save(update_fields=['progreso_total'])
        await sync_to_async(_update)()
