from __future__ import annotations

import logging
from asgiref.sync import sync_to_async
from core.models import UsuarioCarrefourWeb, TareaCatalogacion

logger: logging.Logger = logging.getLogger(__name__)


class CarrefourAuthService:
    """Centraliza login y regionalizacion en carrefour.com.ar via Playwright."""

    async def login(self, page, tarea: TareaCatalogacion) -> None:
        credenciales = await sync_to_async(UsuarioCarrefourWeb.objects.first)()
        if not credenciales:
            raise ValueError("No hay credenciales de Carrefour Web configuradas en Ajustes.")

        mail = credenciales.email
        password = credenciales.clave

        await self._log(tarea, "Ingresando a carrefour.com.ar")
        await page.goto("https://www.carrefour.com.ar/")
        await page.wait_for_timeout(6000)

        try:
            await page.locator("button:has-text('Rechazar todo')").click(timeout=5000)
            await self._log(tarea, "Se denegaron las cookies")
        except Exception:
            await self._log(tarea, "No se encontro boton de cookies, continuando")

        await self._log(tarea, "Abriendo modal de login")
        boton_regio = page.locator("button.carrefourar-regionalizer-1-x-buttonOpenModal")
        await boton_regio.wait_for(state="visible", timeout=10000)
        await boton_regio.click()

        await page.get_by_text("Ingresar con mail y contraseña").click()
        await page.wait_for_timeout(3500)

        try:
            await page.locator("input[placeholder='Ej.: ejemplo@mail.com']").fill(mail)
            await page.wait_for_timeout(2000)
            await page.locator("input[placeholder='Ingrese su contraseña ']").click()
            await page.wait_for_timeout(2000)
            await page.locator("input[placeholder='Ingrese su contraseña ']").fill(password)

            await page.wait_for_timeout(2000)
            await page.get_by_role("button", name="INICIAR SESIÓN").click()
        except Exception:
            logger.warning("Primer intento de login fallo, reintentando")
            await page.locator("input[placeholder='Ej.: ejemplo@mail.com']").fill(mail)
            await page.wait_for_timeout(2000)
            await page.locator("input[placeholder='Ingrese su contraseña ']").fill(password)
            await page.wait_for_timeout(2000)
            await page.get_by_role("button", name="INICIAR SESIÓN").click()

        await page.wait_for_timeout(5000)
        await self._log(tarea, "Inicio de sesion correcto")

    async def regionalizar(self, page, direccion: str, tipo_regio: str, tarea: TareaCatalogacion) -> None:
        await self._log(tarea, f"Iniciando regionalizacion: {direccion} ({tipo_regio})")
        await page.locator("button.carrefourar-regionalizer-1-x-hasStoreSelected").click()
        await page.wait_for_timeout(2000)

        if tipo_regio == "envio":
            await page.locator("button.carrefourar-regionalizer-1-x-buttonTypeOrderShipping").click()
            await page.locator("input[placeholder='Ej: Av. del Libertador 1345']").fill(direccion)
            await page.wait_for_timeout(2500)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(2000)
            await page.get_by_role("button", name="CONTINUAR").click()
            await page.wait_for_timeout(2000)
            await page.get_by_role("button", name="GUARDAR DIRECCIÓN").click()
            await page.wait_for_timeout(10000)
        else:
            await page.locator("button.carrefourar-regionalizer-1-x-buttonTypeOrderDrive").click()
            await page.wait_for_timeout(2000)
            await page.locator("input[placeholder='Ej: Av. del Libertador 1345']").fill(direccion)
            await page.wait_for_timeout(2500)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(2000)
            await page.locator("input[placeholder='Ej: Av. del Libertador 1345']").click()
            await page.keyboard.press("Enter")

            try:
                await page.locator("button.carrefourar-regionalizer-1-x-listStoresButton").first.click()
                await page.wait_for_timeout(2000)
            except Exception:
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(2000)
                await page.keyboard.press("Enter")
                await page.locator("button.carrefourar-regionalizer-1-x-listStoresButton").first.click()
                await page.wait_for_timeout(2000)

            await page.get_by_role("button", name="CONTINUAR").click()
            await page.wait_for_timeout(10000)

        await self._log(tarea, "Regionalizacion finalizada")

    async def _log(self, tarea: TareaCatalogacion, mensaje: str) -> None:
        logger.info(mensaje)
        await sync_to_async(tarea.agregar_log)(mensaje)
