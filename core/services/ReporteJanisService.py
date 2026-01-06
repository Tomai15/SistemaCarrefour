"""
Servicio para generar reportes de Janis usando la API de OMS.
"""
from asgiref.sync import sync_to_async

from core.models import ReporteJanis, TransaccionJanis, UsuarioJanis

from django.conf import settings
import logging
import os
from datetime import datetime, timedelta
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class ReporteJanisService:
    """Servicio para generar reportes de transacciones de Janis."""

    # URL base de la API de Janis OMS
    API_BASE_URL = "https://oms.janis.in/api"

    # Tamaño de página máximo
    PAGE_SIZE = 100

    def __init__(self, ruta_carpeta=None):
        """
        Inicializa el servicio de reportes Janis.

        Args:
            ruta_carpeta: Ruta donde se guardarán los archivos descargados.
                         Si no se proporciona, usa MEDIA_ROOT de Django.
        """
        if ruta_carpeta is None:
            self.ruta_carpeta = settings.MEDIA_ROOT
        else:
            self.ruta_carpeta = ruta_carpeta

        os.makedirs(self.ruta_carpeta, exist_ok=True)

    async def _obtener_credenciales(self):
        """
        Obtiene credenciales de Janis desde la base de datos.

        Returns:
            UsuarioJanis: Objeto con credenciales (api_key, api_secret, client_code)

        Raises:
            ValueError: Si no hay credenciales configuradas
        """
        credenciales = await sync_to_async(UsuarioJanis.objects.first)()
        if not credenciales:
            raise ValueError(
                "No hay credenciales de Janis configuradas. "
                "Por favor, configure las credenciales en el Admin de Django (/admin)."
            )
        return credenciales

    async def generar_reporte(self, fecha_inicio, fecha_fin, reporte_id):
        """
        Genera un reporte de Janis para el rango de fechas especificado.

        Args:
            fecha_inicio: Fecha de inicio en formato DD/MM/YYYY
            fecha_fin: Fecha de fin en formato DD/MM/YYYY
            reporte_id: ID del objeto ReporteJanis en la base de datos

        Returns:
            bool: True si se generó exitosamente, False en caso contrario
        """
        try:
            # Obtener el reporte de la base de datos
            reporte = await sync_to_async(ReporteJanis.objects.get)(id=reporte_id)

            # Actualizar estado a PROCESANDO
            reporte.estado = ReporteJanis.Estado.PROCESANDO
            await sync_to_async(reporte.save)()

            logger.info(f"Generando reporte Janis desde {fecha_inicio} hasta {fecha_fin}")

            # Obtener credenciales desde la base de datos
            credenciales = await self._obtener_credenciales()

            # Descargar pedidos de Janis
            transacciones_df = await sync_to_async(self.descargar_transacciones)(
                fecha_inicio,
                fecha_fin,
                credenciales
            )

            # Guardar transacciones en la base de datos
            cantidad = await self.guardar_transacciones(transacciones_df, reporte)

            # Actualizar estado a COMPLETADO
            reporte.estado = ReporteJanis.Estado.COMPLETADO
            await sync_to_async(reporte.save)()

            logger.info(
                f"Reporte Janis #{reporte_id} generado exitosamente. "
                f"{cantidad} transacciones guardadas."
            )
            return True

        except ReporteJanis.DoesNotExist:
            logger.error(f"Reporte Janis #{reporte_id} no encontrado")
            return False
        except ValueError as e:
            logger.error(f"Error de configuración: {str(e)}")
            try:
                reporte.estado = ReporteJanis.Estado.ERROR
                await sync_to_async(reporte.save)()
            except:
                pass
            return False
        except Exception as e:
            logger.error(f"Error al generar reporte Janis #{reporte_id}: {str(e)}", exc_info=True)
            try:
                reporte.estado = ReporteJanis.Estado.ERROR
                await sync_to_async(reporte.save)()
            except:
                pass
            return False

    async def guardar_transacciones(self, transacciones_df, reporte):
        """
        Guarda las transacciones en la base de datos.

        Args:
            transacciones_df: DataFrame de pandas con las transacciones
                            Columnas esperadas: numero_pedido, numero_transaccion,
                                              fecha_hora, medio_pago, seller, estado
            reporte: Objeto ReporteJanis

        Returns:
            int: Cantidad de transacciones guardadas
        """
        if transacciones_df.empty:
            logger.warning("DataFrame de transacciones vacío, no hay nada que guardar")
            return 0

        transacciones_objetos = []

        for _, row in transacciones_df.iterrows():
            try:
                # Parsear fecha UTC y convertir a hora Argentina (UTC-3)
                fecha_hora_utc = pd.to_datetime(row['fecha_hora'], utc=True)
                fecha_hora = (fecha_hora_utc - timedelta(hours=3)).replace(tzinfo=None)

                transaccion = TransaccionJanis(
                    numero_pedido=str(row.get('numero_pedido', '')),
                    numero_transaccion=str(row.get('numero_transaccion', '')),
                    fecha_hora=fecha_hora,
                    medio_pago=str(row.get('medio_pago', 'N/A')),
                    seller=str(row.get('seller', 'No encontrado')),
                    estado=str(row.get('estado', 'Desconocido')),
                    reporte=reporte
                )
                transacciones_objetos.append(transaccion)

            except Exception as e:
                logger.warning(f"Error procesando transacción {row.get('numero_pedido', 'N/A')}: {e}")
                continue

        # Inserción en lote (eficiente para grandes volúmenes)
        if transacciones_objetos:
            await sync_to_async(TransaccionJanis.objects.bulk_create)(
                transacciones_objetos,
                batch_size=1000
            )
            logger.info(f"Guardadas {len(transacciones_objetos)} transacciones Janis")

        return len(transacciones_objetos)

    def _get_headers(self, credenciales, page=1):
        """
        Genera los headers para las requests a la API de Janis.

        Args:
            credenciales: Objeto UsuarioJanis con las credenciales
            page: Número de página para paginación

        Returns:
            dict: Headers para la request
        """
        return {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'janis-api-key': credenciales.api_key,
            'janis-api-secret': credenciales.api_secret,
            'janis-client': credenciales.client_code,
            'x-janis-page': str(page),
            'x-janis-page-size': str(self.PAGE_SIZE)
        }

    def _formatear_fecha_iso(self, fecha):
        """
        Formatea una fecha a formato ISO 8601 para la API de Janis.

        Args:
            fecha: datetime object

        Returns:
            str: Fecha en formato ISO 8601 (ej: 2024-01-15T00:00:00.000Z)
        """
        return fecha.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def descargar_transacciones(self, fecha_inicio_str, fecha_fin_str, credenciales):
        """
        Descarga transacciones de Janis para el rango de fechas.

        Args:
            fecha_inicio_str: Fecha de inicio en formato DD/MM/YYYY
            fecha_fin_str: Fecha de fin en formato DD/MM/YYYY
            credenciales: Objeto UsuarioJanis con las credenciales

        Returns:
            DataFrame: DataFrame con las transacciones descargadas.
                      Columnas:
                      - numero_pedido: str (commerceId)
                      - numero_transaccion: str (commerceSequentialId)
                      - fecha_hora: datetime (commerceDateCreated)
                      - medio_pago: str (paymentSystemName del primer payment)
                      - seller: str (seller.name)
                      - estado: str (status)
        """
        logger.info(f"Descargando transacciones Janis desde {fecha_inicio_str} hasta {fecha_fin_str}")

        # Parsear fechas y convertir a UTC (Argentina es UTC-3, sumamos 3 horas)
        fecha_desde = datetime.strptime(fecha_inicio_str, "%d/%m/%Y") + timedelta(hours=3)
        fecha_hasta = datetime.strptime(fecha_fin_str, "%d/%m/%Y") + timedelta(
            hours=23, minutes=59, seconds=59
        ) + timedelta(hours=3)

        # URL del endpoint
        url = f"{self.API_BASE_URL}/order"

        # Parámetros de filtro por fecha
        params = {
            'filters[commerceDateCreatedRange][from]': self._formatear_fecha_iso(fecha_desde),
            'filters[commerceDateCreatedRange][to]': self._formatear_fecha_iso(fecha_hasta),
            'sortBy': 'commerceDateCreated',
            'sortDirection': 'asc'
        }

        todos_los_pedidos = []
        page = 1
        total_pages = None

        while True:
            headers = self._get_headers(credenciales, page)

            try:
                response = requests.get(url, headers=headers, params=params, timeout=60)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error en request a Janis API: {e}")
                raise

            # Obtener total de registros del header
            total_registros = int(response.headers.get('x-janis-total', 0))

            if total_pages is None:
                total_pages = (total_registros + self.PAGE_SIZE - 1) // self.PAGE_SIZE
                logger.info(f"Total de registros: {total_registros}, páginas: {total_pages}")

            # Parsear respuesta
            data = response.json()

            if not data:
                break

            todos_los_pedidos.extend(data)
            logger.info(f"Página {page}/{total_pages} - {len(data)} pedidos descargados")

            page += 1

            # Verificar si hay más páginas
            if page > total_pages:
                break

        logger.info(f"Total pedidos descargados: {len(todos_los_pedidos)}")

        # Procesar pedidos y convertir a DataFrame
        transacciones = []
        for pedido in todos_los_pedidos:
            try:
                # Extraer medio de pago del primer payment si existe
                payments = pedido.get('payments', [])
                medio_pago = 'N/A'
                if payments and len(payments) > 0:
                    medio_pago = payments[0].get('paymentSystemName', 'N/A')

                # Extraer seller
                seller_data = pedido.get('seller', {})
                seller_name = 'No encontrado'
                if seller_data:
                    seller_name = seller_data.get('name', 'No encontrado')

                transaccion = {
                    'numero_pedido': pedido.get('commerceId', ''),
                    'numero_transaccion': pedido.get('commerceSequentialId', ''),
                    'fecha_hora': pedido.get('commerceDateCreated', ''),
                    'medio_pago': medio_pago,
                    'seller': seller_name,
                    'estado': pedido.get('status', 'Desconocido')
                }
                transacciones.append(transaccion)
            except Exception as e:
                logger.warning(f"Error procesando pedido {pedido.get('commerceId', 'N/A')}: {e}")
                continue

        # Crear DataFrame
        df = pd.DataFrame(transacciones)

        if not df.empty:
            # Eliminar duplicados por numero_pedido
            duplicados = len(df) - len(df.drop_duplicates(subset=['numero_pedido']))
            if duplicados > 0:
                logger.info(f"Eliminando {duplicados} pedidos duplicados")
                df = df.drop_duplicates(subset=['numero_pedido'])

            # Exportar archivo Excel
            ruta_carpeta = os.path.join(self.ruta_carpeta, "janis")
            os.makedirs(ruta_carpeta, exist_ok=True)

            archivo_final = os.path.join(
                ruta_carpeta,
                f"pedidos_janis_{fecha_desde.date()}_a_{fecha_hasta.date()}.xlsx"
            )
            df.to_excel(archivo_final, index=False)
            logger.info(f"Archivo final exportado a: {archivo_final}")

        return df

    def importar_desde_excel(self, archivo, reporte):
        """
        Importa transacciones desde un archivo Excel.

        Args:
            archivo: Archivo Excel subido (InMemoryUploadedFile o similar)
            reporte: Objeto ReporteJanis donde guardar las transacciones

        Returns:
            int: Cantidad de transacciones importadas

        Raises:
            ValueError: Si el archivo no tiene las columnas requeridas
            Exception: Si ocurre algún error durante la importación
        """
        logger.info(f"Importando transacciones desde Excel para reporte #{reporte.id}")

        try:
            # Leer el archivo Excel
            df = pd.read_excel(archivo)

            logger.info(f"Archivo leído: {len(df)} filas, columnas: {list(df.columns)}")

            # Columnas requeridas
            columnas_requeridas = ['commerceId', 'commerceSequentialId', 'commerceDateCreated', 'paymentSystemName', 'shippingWarehouseName', 'status']

            # Verificar que existan las columnas (o ajustar según tu Excel)
            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            if columnas_faltantes:
                raise ValueError(
                    f"El archivo no tiene las columnas requeridas: {columnas_faltantes}. "
                    f"Columnas encontradas: {list(df.columns)}"
                )

            # Crear objetos de transacción
            transacciones_objetos = []

            for _, row in df.iterrows():
                try:
                    # Parsear fecha
                    fecha_hora = pd.to_datetime(row['commerceDateCreated'])
                    if pd.isna(fecha_hora):
                        fecha_hora = datetime.now()

                    # Convertir números a string sin decimales
                    # (pandas lee números de Excel como float: 12345 -> 12345.0)
                    numero_pedido = row.get('commerceId', '')
                    if pd.notna(numero_pedido):
                        if isinstance(numero_pedido, float) and numero_pedido.is_integer():
                            numero_pedido = int(numero_pedido)
                    numero_pedido = str(numero_pedido).strip()

                    numero_transaccion = row.get('commerceSequentialId', '')
                    if pd.notna(numero_transaccion):
                        if isinstance(numero_transaccion, float) and numero_transaccion.is_integer():
                            numero_transaccion = int(numero_transaccion)
                    numero_transaccion = str(numero_transaccion).strip()

                    transaccion = TransaccionJanis(
                        numero_pedido=numero_pedido,
                        numero_transaccion=numero_transaccion,
                        fecha_hora=fecha_hora,
                        medio_pago=str(row.get('paymentSystemName', 'N/A')).strip(),
                        seller=str(row.get('shippingWarehouseName', 'No encontrado')).strip(),
                        estado=str(row.get('status', 'Desconocido')).strip(),
                        reporte=reporte
                    )
                    transacciones_objetos.append(transaccion)

                except Exception as e:
                    logger.warning(f"Error procesando fila {row.get('numero_pedido', 'N/A')}: {e}")
                    continue

            # Inserción en lote
            if transacciones_objetos:
                TransaccionJanis.objects.bulk_create(
                    transacciones_objetos,
                    batch_size=1000
                )
                logger.info(f"Importadas {len(transacciones_objetos)} transacciones Janis")

            return len(transacciones_objetos)

        except Exception as e:
            logger.error(f"Error al importar Excel: {str(e)}", exc_info=True)
            raise
