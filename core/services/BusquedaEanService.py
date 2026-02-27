from __future__ import annotations

import asyncio
import csv
import logging
import os
from datetime import datetime

import pandas as pd
from asgiref.sync import sync_to_async
from django.conf import settings
from playwright.async_api import async_playwright

from core.models import TareaCatalogacion
from core.services.CarrefourAuthService import CarrefourAuthService

logger: logging.Logger = logging.getLogger(__name__)


class BusquedaEanService:
    """Servicio que busca una lista de EANs en carrefour.com.ar usando Playwright."""

    def __init__(self) -> None:
        self._auth_service = CarrefourAuthService()

    async def ejecutar(
        self,
        tarea: TareaCatalogacion,
        eans: list[str],
        direccion: str,
        tipo_regio: str,
        n_workers: int = 3,
        headless: bool = True,
    ) -> None:
        """
        Lanza un browser headless, inicia sesion, regionaliza y luego busca cada
        EAN de forma concurrente con *n_workers* workers.

        Actualiza ``tarea`` en cada paso para que el frontend pueda seguir el
        progreso via polling.
        """
        await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.PROCESANDO)

        total = len(eans)
        await self._set_progreso_total(tarea, total)

        if total == 0:
            await self._log(tarea, "No hay EANs para procesar.")
            await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.COMPLETADO)
            return

        await self._log(tarea, f"Iniciando busqueda concurrente: {total} EANs, {n_workers} workers.")

        carpeta = os.path.join(settings.MEDIA_ROOT, 'catalogacion')
        os.makedirs(carpeta, exist_ok=True)
        storage_path = os.path.join(settings.MEDIA_ROOT, 'auth_state_eans.json')

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=headless)

                # --- Login y regionalizacion con contexto base ---
                base_ctx = await browser.new_context()
                base_page = await base_ctx.new_page()

                await self._auth_service.login(base_page, tarea)
                await self._auth_service.regionalizar(base_page, direccion, tipo_regio, tarea)

                await base_ctx.storage_state(path=storage_path)
                await base_ctx.close()

                # --- Cola de EANs ---
                queue: asyncio.Queue[str] = asyncio.Queue()
                for ean in eans:
                    queue.put_nowait(str(ean))

                results: list[tuple[str, str]] = []
                lock = asyncio.Lock()

                # --- Worker ---
                async def worker(wid: int) -> None:
                    ctx = await browser.new_context(storage_state=storage_path)
                    page = await ctx.new_page()
                    try:
                        while True:
                            try:
                                ean = await asyncio.wait_for(queue.get(), timeout=1.0)
                            except asyncio.TimeoutError:
                                break

                            async with lock:
                                await self._log(tarea, f"[W{wid}] Buscando {ean}...")

                            estado = "NO ENCONTRADO"
                            try:
                                await page.goto(
                                    f"https://www.carrefour.com.ar/{ean}?q={ean}&map=ft"
                                )
                                await page.wait_for_timeout(3000)

                                try:
                                    no_encontrado_visible = await page.locator(
                                        "text=Disculpanos, no encontramos productos que coincidan con tu búsqueda"
                                    ).is_visible()
                                    if no_encontrado_visible:
                                        estado = "NO ENCONTRADO"
                                    else:
                                        # Verificar si es de un seller externo
                                        es_externo = await page.locator(
                                            'span.valtech-carrefourar-external-seller-0-x-containerExternalSeller'
                                        ).first.is_visible()
                                        if es_externo:
                                            # Extraer nombre del seller
                                            nombre_seller = ""
                                            try:
                                                nombre_seller = await page.locator(
                                                    '[data-specification-name="Marca Seller Partner"]'
                                                ).first.get_attribute('data-specification-value')
                                            except Exception:
                                                pass
                                            if nombre_seller:
                                                estado = f"ENCONTRADO - SELLER EXTERNO ({nombre_seller})"
                                            else:
                                                estado = "ENCONTRADO - SELLER EXTERNO"
                                        else:
                                            estado = "ENCONTRADO"
                                except Exception as e:
                                    async with lock:
                                        await self._log(tarea, f"[W{wid}] Error revisando DOM para {ean}: {e}")

                            except Exception as e:
                                async with lock:
                                    await self._log(tarea, f"[W{wid}] Error navegando a {ean}: {e}")

                            finally:
                                async with lock:
                                    results.append((ean, estado))
                                    await self._incrementar_progreso(tarea)
                                    progreso = tarea.progreso_actual
                                    await self._log(
                                        tarea,
                                        f"[W{wid}] {ean}: {estado} ({progreso}/{total})",
                                    )
                                queue.task_done()
                    finally:
                        await ctx.close()

                # --- Lanzar workers ---
                workers_count = max(1, min(n_workers, total))
                tasks = [asyncio.create_task(worker(i + 1)) for i in range(workers_count)]

                await queue.join()

                for t in tasks:
                    if not t.done():
                        t.cancel()

                await browser.close()

            # --- Generar archivos de resultados ---
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dir_segura = direccion.replace(' ', '_')

            nombre_csv = f"resultados_{dir_segura}_{timestamp}.csv"
            ruta_csv = os.path.join(carpeta, nombre_csv)

            nombre_excel = f"resultados_{dir_segura}_{timestamp}.xlsx"
            ruta_excel = os.path.join(carpeta, nombre_excel)

            with open(ruta_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['EAN', 'Estado'])
                writer.writerows(results)

            pd.read_csv(ruta_csv).to_excel(ruta_excel, index=False)

            await self._log(tarea, f"Proceso finalizado. Archivo guardado: {nombre_excel}")
            await sync_to_async(self._guardar_archivo)(tarea, f'catalogacion/{nombre_excel}')
            await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.COMPLETADO)

        except Exception as e:
            logger.error(f"Error en BusquedaEanService: {e}", exc_info=True)
            await self._log(tarea, f"Error fatal: {e}")
            await sync_to_async(self._actualizar_estado)(tarea, TareaCatalogacion.Estado.ERROR)

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    async def _log(self, tarea: TareaCatalogacion, mensaje: str) -> None:
        logger.info(mensaje)
        await sync_to_async(tarea.agregar_log)(mensaje)

    async def _incrementar_progreso(self, tarea: TareaCatalogacion) -> None:
        def _update() -> None:
            tarea.progreso_actual += 1
            tarea.save(update_fields=['progreso_actual'])
        await sync_to_async(_update)()

    async def _set_progreso_total(self, tarea: TareaCatalogacion, total: int) -> None:
        def _update() -> None:
            tarea.progreso_total = total
            tarea.save(update_fields=['progreso_total'])
        await sync_to_async(_update)()

    @staticmethod
    def _actualizar_estado(tarea: TareaCatalogacion, estado: str) -> None:
        tarea.estado = estado
        tarea.save(update_fields=['estado'])

    @staticmethod
    def _guardar_archivo(tarea: TareaCatalogacion, ruta_relativa: str) -> None:
        tarea.archivo_resultado = ruta_relativa
        tarea.save(update_fields=['archivo_resultado'])
