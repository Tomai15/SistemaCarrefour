from asgiref.sync import sync_to_async

from core.models import UsuarioPayway, ReportePayway, TransaccionPayway
from playwright.async_api import async_playwright
from django.db import transaction
from django.conf import settings
from django.utils import timezone
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ReportePaywayService:
    """Servicio para generar reportes de transacciones de Payway."""

    TIEMPO_DE_ESPERA = 900000  # 900 segundos (15 minutos)

    def __init__(self, ruta_carpeta=None):
        """
        Inicializa el servicio de reportes Payway.

        Args:
            ruta_carpeta: Ruta donde se guardarán los archivos descargados.
                         Si no se proporciona, usa MEDIA_ROOT de Django.
        """
        self.lista_archivos_excel = []
        self.dias_con_error = []
        self.fecha_formato_mostrar = ""
        self.fecha_formato_guardado = ""
        self.usuario = ""
        self.contrasena = ""

        # Si no se proporciona ruta, usar MEDIA_ROOT
        if ruta_carpeta is None:
            self.ruta_carpeta = settings.MEDIA_ROOT
        else:
            self.ruta_carpeta = ruta_carpeta

        # Asegurar que el directorio existe
        os.makedirs(self.ruta_carpeta, exist_ok=True)

    async def entrar_pagina(self, pagina):
        """
        Realiza el login en la plataforma Payway.

        Args:
            pagina: Objeto Page de Playwright.
        """
        # Navegar a la página de login
        await pagina.goto("https://ventasonline.payway.com.ar/sac/SAC")
        await pagina.wait_for_load_state("load")

        # Completar el formulario de login
        await pagina.fill("input[name='usuariosps']", self.usuario)
        await pagina.fill("input[name='passwordsps']", self.contrasena)

        # Hacer clic en el botón de login
        await pagina.click("input[id='image1']")
        await pagina.wait_for_load_state("networkidle", timeout=self.TIEMPO_DE_ESPERA)

    async def descargar_y_convertir(self, pagina, fecha_formato_guardado, hora_inicio, minuto_inicio,
                               hora_fin, minuto_fin, etiqueta):
        """
        Descarga el CSV, lo convierte a Excel y lo almacena en la lista de archivos.

        Args:
            pagina: Objeto Page de Playwright.
            fecha_formato_guardado: Fecha en formato YYYY-MM-DD.
            hora_inicio: Hora de inicio (00-23).
            minuto_inicio: Minuto de inicio (00-59).
            hora_fin: Hora de fin (00-23).
            minuto_fin: Minuto de fin (00-59).
            etiqueta: Etiqueta descriptiva del archivo (mañana, tarde, etc.).

        Returns:
            str: Ruta del archivo Excel generado, o None si falla.
        """
        try:
            # Esperar a que los campos de hora estén disponibles
            await pagina.wait_for_selector("input[name='sacparam_horaini']", timeout=self.TIEMPO_DE_ESPERA)

            # Verificar si existen antes de llenarlos
            if await pagina.locator("input[name='sacparam_horaini']").count() > 0:
                await pagina.fill("input[name='sacparam_horaini']", hora_inicio)
                await pagina.fill("input[name='sacparam_minutoini']", minuto_inicio)
                await pagina.fill("input[name='sacparam_horafin']", hora_fin)
                await pagina.fill("input[name='sacparam_minutofin']", minuto_fin)
            else:
                logger.error("No se encontraron los campos de hora en la página.")
                return None

            async with pagina.expect_download() as informacion_descarga:
                await pagina.click("input[name='b_downloadform']", timeout=self.TIEMPO_DE_ESPERA)

            archivo_descargado = await informacion_descarga.value
            nombre_csv = f"transacciones_{fecha_formato_guardado}_{etiqueta}.csv"
            ruta_csv = os.path.join(self.ruta_carpeta, nombre_csv)
            await archivo_descargado.save_as(ruta_csv)

            # Convertir CSV a Excel
            datos = pd.read_csv(ruta_csv, delimiter="\t", encoding="ISO-8859-1", dtype=str, index_col=False)
            datos.columns = datos.columns.str.strip()
            nombre_excel = nombre_csv.replace(".csv", ".xlsx")
            ruta_excel = os.path.join(self.ruta_carpeta, nombre_excel)
            datos.to_excel(ruta_excel, index=False)

            # Eliminar el CSV original para ahorrar espacio
            os.remove(ruta_csv)

            logger.info(f"Archivo convertido exitosamente: {nombre_excel}")
            return ruta_excel

        except Exception as e:
            logger.error(f"Error al descargar y convertir archivo: {e}", exc_info=True)
            return None

    async def buscar_dia(self, fecha, pagina):
        """
        Configura la búsqueda para un día completo (00:00 - 23:59).

        Args:
            fecha: Objeto datetime con la fecha a buscar.
            pagina: Objeto Page de Playwright.
        """
        self.fecha_formato_mostrar = fecha.strftime("%d/%m/%Y")
        self.fecha_formato_guardado = fecha.strftime("%Y-%m-%d")

        # Establecer la búsqueda para todo el día (00:00 - 23:59)
        await pagina.fill("input[name='sacparam_fechaini']", self.fecha_formato_mostrar)
        await pagina.fill("input[name='sacparam_fechafin']", self.fecha_formato_mostrar)
        await pagina.fill("input[name='sacparam_horaini']", "00")
        await pagina.fill("input[name='sacparam_minutoini']", "00")
        await pagina.fill("input[name='sacparam_horafin']", "23")
        await pagina.fill("input[name='sacparam_minutofin']", "59")
        await pagina.click("input[name='b_consultaform']", timeout=self.TIEMPO_DE_ESPERA)
        await pagina.wait_for_load_state("networkidle", timeout=self.TIEMPO_DE_ESPERA)

    async def error_al_buscar_dia(self, pagina):
        """
        Verifica si ocurrió un error en la búsqueda.

        Args:
            pagina: Objeto Page de Playwright.

        Returns:
            bool: True si hay un error, False en caso contrario.
        """
        mensaje_error = pagina.locator("p:has-text('Ha ocurrido un error')")
        return await mensaje_error.count() > 0

    async def transacciones_superadas(self, pagina):
        """
        Verifica si se superó el límite de 5000 transacciones.

        Args:
            pagina: Objeto Page de Playwright.

        Returns:
            bool: True si se superó el límite, False en caso contrario.
        """
        mensaje_error = pagina.locator("td.textonaranja")
        count = await mensaje_error.count()
        if count > 0:
            text_content = await mensaje_error.text_content()
            return "5000 transacciones" in text_content
        return False

    async def procesar_dia(self, pagina, fecha_actual):
        """
        Procesa las transacciones de un día, dividiéndolas si es necesario.

        Args:
            pagina: Objeto Page de Playwright.
            fecha_actual: Objeto datetime con la fecha a procesar.

        Returns:
            datetime: La fecha procesada.
        """
        if await self.error_al_buscar_dia(pagina):
            logger.warning(f"El día {fecha_actual} arrojó un error. Se reintentará más tarde.")
            self.dias_con_error.append(fecha_actual)
            await self.entrar_pagina(pagina)
        else:
            if await self.transacciones_superadas(pagina):
                logger.info(f"Más de 5000 transacciones el {fecha_actual}, dividiendo en mañana y tarde")

                for parte_del_dia, hora_inicio, minuto_inicio, hora_fin, minuto_fin in [
                    ("mañana", "00", "00", "11", "59"),
                    ("tarde", "12", "00", "23", "59")
                ]:
                    archivo = await self.descargar_y_convertir(
                        pagina, self.fecha_formato_guardado, hora_inicio, minuto_inicio,
                        hora_fin, minuto_fin, parte_del_dia
                    )
                    if archivo:
                        self.lista_archivos_excel.append(archivo)

                    # Rehacer la consulta para ver si hay más de 5000 registros en la mañana o tarde
                    await pagina.fill("input[name='sacparam_horaini']", hora_inicio)
                    await pagina.fill("input[name='sacparam_minutoini']", minuto_inicio)
                    await pagina.fill("input[name='sacparam_horafin']", hora_fin)
                    await pagina.fill("input[name='sacparam_minutofin']", minuto_fin)
                    await pagina.click("input[name='b_consultaform']", timeout=self.TIEMPO_DE_ESPERA)
                    await pagina.wait_for_load_state("networkidle", timeout=self.TIEMPO_DE_ESPERA)

                    if await self.transacciones_superadas(pagina):
                        logger.info(
                            f"Más de 5000 transacciones en {parte_del_dia} del {self.fecha_formato_mostrar}, "
                            f"dividiendo en 4 intervalos"
                        )

                        for sub_parte, h_inicio, m_inicio, h_fin, m_fin in [
                            ("madrugada", "00", "00", "05", "59"),
                            ("mañana", "06", "00", "11", "59"),
                            ("tarde", "12", "00", "17", "59"),
                            ("noche", "18", "00", "23", "59")
                        ]:
                            archivo = await self.descargar_y_convertir(
                                pagina, self.fecha_formato_guardado, h_inicio, m_inicio,
                                h_fin, m_fin, sub_parte
                            )
                            if archivo:
                                self.lista_archivos_excel.append(archivo)

            else:
                archivo = await self.descargar_y_convertir(
                    pagina, self.fecha_formato_guardado, "00", "00", "23", "59", "completo"
                )
                if archivo:
                    self.lista_archivos_excel.append(archivo)

            logger.info(f"Finalizado el día {fecha_actual}")

        return fecha_actual

    async def generar_reporte(self, fecha_inicio, fecha_fin,nuevo_reporte):
        """
        Genera un reporte completo de transacciones para un rango de fechas.

        Args:
            fecha_inicio: Fecha de inicio en formato DD/MM/YYYY.
            fecha_fin: Fecha de fin en formato DD/MM/YYYY.

        Returns:
            int: ID del reporte generado, o None si falla.
        """
        # Parsear fechas
        fecha_inicio_dt = datetime.strptime(fecha_inicio, "%d/%m/%Y")
        fecha_fin_dt = datetime.strptime(fecha_fin, "%d/%m/%Y")


        navegador_web = None

        try:
            # Obtener credenciales
            credenciales_payway = await sync_to_async(UsuarioPayway.objects.first)()
            if not credenciales_payway:
                raise ValueError("No hay credenciales de Payway configuradas en la base de datos")

            self.usuario = credenciales_payway.usuario
            self.contrasena = credenciales_payway.clave

            # Actualizar estado a PROCESANDO
            nuevo_reporte.estado = ReportePayway.Estado.PROCESANDO
            await sync_to_async(nuevo_reporte.save)()

            logger.info(f"Iniciando generación de reporte {nuevo_reporte.id} desde {fecha_inicio} hasta {fecha_fin}")

            async with async_playwright() as navegador:
                navegador_web = await navegador.chromium.launch(headless=False)
                pagina = await navegador_web.new_page()

                await self.entrar_pagina(pagina)

                # Procesar cada día en el rango
                fecha_actual = fecha_inicio_dt
                while fecha_actual <= fecha_fin_dt:
                    await self.buscar_dia(fecha_actual, pagina)
                    await self.procesar_dia(pagina, fecha_actual)
                    fecha_actual += timedelta(days=1)

                # Reintentar días con error
                logger.info("Descarga de días terminada. Se procede a reintentar los días con error")
                for dia_con_error in self.dias_con_error:
                    logger.info(f"Se reintenta el día {dia_con_error}")
                    await self.buscar_dia(dia_con_error, pagina)
                    await self.procesar_dia(pagina, dia_con_error)

                logger.info("Descargas y conversiones a Excel completadas. Uniendo archivos...")

                # Verificar que hay archivos para procesar
                if not self.lista_archivos_excel:
                    raise ValueError("No se descargaron archivos de transacciones")

                # Unir todos los archivos Excel en uno solo
                lista_datos = [pd.read_excel(archivo) for archivo in self.lista_archivos_excel]
                datos_finales = pd.concat(lista_datos, ignore_index=True)

                # Eliminar todas las columnas sobrantes
                datos_finales = datos_finales[["id oper.", "Fecha original", "Monto", "Estado", "Tarjeta"]]

                # Insertar transacciones en la base de datos usando transacción atómica
                logger.info(f"Insertando {len(datos_finales)} transacciones en la base de datos...")

                await sync_to_async(self.guardar_transacciones_sincrinico)(datos_finales,nuevo_reporte)


                logger.info(f"Finalizada la generación del reporte {nuevo_reporte.id}")

                return nuevo_reporte.id

        except Exception as e:
            logger.error(f"Error al generar reporte: {e}", exc_info=True)

            # Actualizar estado a ERROR
            nuevo_reporte.estado = ReportePayway.Estado.ERROR
            await sync_to_async(nuevo_reporte.save)()

            raise

        finally:
            # El navegador se cierra automáticamente al salir del async with
            # pero registramos que terminó el proceso
            if navegador_web:
                logger.info("Proceso de navegación finalizado")

    def guardar_transacciones_sincrinico(self,transacciones,reporte):
        reportes_objeto = []
        for indice, transaccion in transacciones.iterrows():
            # Parsear la fecha del formato DD/MM/YYYY HH:MM:SS al formato de Django
            fecha_str = transaccion["Fecha original"]
            try:
                fecha_parseada = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
            except ValueError:
                # Si falla, intentar sin segundos
                try:
                    fecha_parseada = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
                except ValueError:
                    logger.error(f"Error al parsear fecha: {fecha_str}")
                    continue

            # Hacer el datetime timezone-aware para evitar warnings
            fecha_parseada = timezone.make_aware(fecha_parseada)

            # Convertir formato argentino de decimal (coma) a formato internacional (punto)
            monto_str = str(transaccion["Monto"]).replace(',', '.')

            transacion_objeto = TransaccionPayway(
                numero_transaccion=str(transaccion["id oper."]).strip(),
                fecha_hora=fecha_parseada,
                monto=monto_str,
                estado=str(transaccion["Estado"]).strip(),
                tarjeta=str(transaccion["Tarjeta"]).strip(),
                reporte=reporte
            )
            reportes_objeto.append(transacion_objeto)
            # Log cada 100 transacciones para no saturar el log
            if (indice + 1) % 100 == 0:
                logger.info(f"Insertadas {indice + 1} transacciones...")

        # Insertar todas las transacciones
        TransaccionPayway.objects.bulk_create(reportes_objeto,batch_size=1000)

        # Actualizar estado a COMPLETADO
        reporte.estado = ReportePayway.Estado.COMPLETADO
        reporte.save()
