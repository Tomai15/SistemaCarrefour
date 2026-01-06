from asgiref.sync import sync_to_async

from core.models import ReporteVtex, TransaccionVtex, UsuarioVtex

from django.conf import settings
import logging
import os
from datetime import datetime, timedelta
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class ReporteVtexService:
    """Servicio para generar reportes de transacciones de VTEX."""

    TIEMPO_DE_ESPERA = 900000  # 900 segundos (15 minutos)

    # Rate limiting: 90 requests/segundo = 5400/minuto (margen de seguridad sobre 6000)
    RATE_LIMIT_PER_SECOND = 90
    # Máximo de conexiones simultáneas abiertas
    MAX_CONCURRENT_CONNECTIONS = 50

    def __init__(self, ruta_carpeta=None):
        """
        Inicializa el servicio de reportes VTEX.

        Args:
            ruta_carpeta: Ruta donde se guardarán los archivos descargados.
                         Si no se proporciona, usa MEDIA_ROOT de Django.
        """
        # Si no se proporciona ruta, usar MEDIA_ROOT
        if ruta_carpeta is None:
            self.ruta_carpeta = settings.MEDIA_ROOT
        else:
            self.ruta_carpeta = ruta_carpeta

        # Asegurar que el directorio existe
        os.makedirs(self.ruta_carpeta, exist_ok=True)

        # Rate limiter y semáforo se inicializan en el contexto async
        self._rate_limiter = None
        self._semaphore = None

    def _init_async_controls(self):
        """Inicializa rate limiter y semáforo para contexto async."""
        if self._rate_limiter is None:
            self._rate_limiter = AsyncLimiter(self.RATE_LIMIT_PER_SECOND, 1)
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_CONNECTIONS)

    async def _obtener_credenciales(self):
        """
        Obtiene credenciales de VTEX desde la base de datos.

        Returns:
            UsuarioVtex: Objeto con credenciales (app_key, app_token, account_name)

        Raises:
            ValueError: Si no hay credenciales configuradas
        """
        credenciales = await sync_to_async(UsuarioVtex.objects.first)()
        if not credenciales:
            raise ValueError(
                "No hay credenciales de VTEX configuradas. "
                "Por favor, configure las credenciales en el Admin de Django (/admin)."
            )
        return credenciales

    async def generar_reporte(self, fecha_inicio, fecha_fin, reporte_id, filtros=None):
        """
        Genera un reporte de VTEX para el rango de fechas especificado.

        Args:
            fecha_inicio: Fecha de inicio en formato DD/MM/YYYY
            fecha_fin: Fecha de fin en formato DD/MM/YYYY
            reporte_id: ID del objeto ReporteVtex en la base de datos
            filtros: Diccionario con filtros a aplicar (ej: {'estados': ['invoiced', 'canceled']})

        Returns:
            bool: True si se generó exitosamente, False en caso contrario
        """
        try:
            # Obtener el reporte de la base de datos
            reporte = await sync_to_async(ReporteVtex.objects.get)(id=reporte_id)

            # Actualizar estado a PROCESANDO
            reporte.estado = ReporteVtex.Estado.PROCESANDO
            await sync_to_async(reporte.save)()

            logger.info(f"Generando reporte VTEX desde {fecha_inicio} hasta {fecha_fin}")
            if filtros:
                logger.info(f"Filtros aplicados: {filtros}")

            # Obtener credenciales desde la base de datos
            credenciales = await self._obtener_credenciales()

            # Descargar pedidos (envolver método síncrono en sync_to_async)
            pedidos_vtex = await sync_to_async(self.descargarVtex)(
                fecha_inicio,
                fecha_fin,
                credenciales,
                filtros
            )

            # Guardar transacciones en la base de datos
            cantidad = await self.guardar_transacciones(pedidos_vtex, reporte)

            # Actualizar estado a COMPLETADO
            reporte.estado = ReporteVtex.Estado.COMPLETADO
            await sync_to_async(reporte.save)()

            logger.info(
                f"Reporte VTEX #{reporte_id} generado exitosamente. "
                f"{cantidad} transacciones guardadas."
            )
            return True

        except ReporteVtex.DoesNotExist:
            logger.error(f"Reporte VTEX #{reporte_id} no encontrado")
            return False
        except ValueError as e:
            logger.error(f"Error de configuración: {str(e)}")
            try:
                reporte.estado = ReporteVtex.Estado.ERROR
                await sync_to_async(reporte.save)()
            except:
                pass
            return False
        except Exception as e:
            logger.error(f"Error al generar reporte VTEX #{reporte_id}: {str(e)}", exc_info=True)
            try:
                reporte.estado = ReporteVtex.Estado.ERROR
                await sync_to_async(reporte.save)()
            except:
                pass
            return False

    async def guardar_transacciones(self, transacciones_df, reporte):
        """
        Guarda las transacciones en la base de datos.

        Args:
            transacciones_df: DataFrame de pandas con las transacciones
                            Columnas esperadas: orderId, sequence, creationDate,
                                              paymentNames, seller, statusDescription, totalValue
            reporte: Objeto ReporteVtex

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
                fecha_hora_utc = pd.to_datetime(row['creationDate'], utc=True)
                fecha_hora = (fecha_hora_utc - timedelta(hours=3)).replace(tzinfo=None)

                # Obtener el valor del pedido (viene en centavos, dividir por 100)
                valor_raw = row.get('totalValue', None)
                valor = None
                if valor_raw is not None:
                    try:
                        valor = float(valor_raw) / 100  # VTEX devuelve valores en centavos
                    except (ValueError, TypeError):
                        valor = None

                transaccion = TransaccionVtex(
                    numero_pedido=str(row['orderId']),
                    numero_transaccion=str(row['sequence']),
                    fecha_hora=fecha_hora,
                    medio_pago=str(row.get('paymentNames', 'N/A')),
                    seller=str(row.get('seller', 'No encontrado')),
                    estado=str(row.get('statusDescription', 'Desconocido')),
                    valor=valor,
                    reporte=reporte
                )
                transacciones_objetos.append(transaccion)

            except Exception as e:
                logger.warning(f"Error procesando transacción {row.get('orderId', 'N/A')}: {e}")
                continue

        # Inserción en lote (eficiente para grandes volúmenes)
        if transacciones_objetos:
            await sync_to_async(TransaccionVtex.objects.bulk_create)(
                transacciones_objetos,
                batch_size=1000
            )
            logger.info(f"Guardadas {len(transacciones_objetos)} transacciones VTEX")

        return len(transacciones_objetos)

    def formatear(self, fecha):
        """Formatea fecha para la API de VTEX"""
        return fecha.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def get_pedidos(self, ini, fin, url, headers, filtros=None):
        """
        Hace una request y devuelve los pedidos + cantidad de páginas.

        Args:
            ini: Fecha inicio
            fin: Fecha fin
            url: URL de la API
            headers: Headers con credenciales
            filtros: Diccionario con filtros a aplicar (ej: {'estados': ['invoiced']})

        Returns:
            tuple: (lista de pedidos, cantidad de páginas)
        """
        params = {
            "f_creationDate": f"creationDate:[{self.formatear(ini)} TO {self.formatear(fin)}]",
            "page": 1,
            "per_page": 100,
            "orderBy": "creationDate,asc"
        }

        # Aplicar filtro de estados si se proporcionó
        if filtros and filtros.get('estados'):
            estados = filtros['estados']
            # VTEX acepta múltiples estados separados por coma
            estados_str = ','.join(estados)
            params["f_status"] = estados_str

        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        return data.get("list", []), data.get("paging", {}).get("pages", 0)

    async def buscar_seller_async(self, session, order_id, url_base, headers):
        """
        Busca el seller de un pedido específico con rate limiting automático.

        El rate limiter funciona como un "balde de fichas":
        - Tenemos 90 fichas por segundo
        - Cada request consume 1 ficha
        - Si no hay fichas, espera automáticamente hasta que haya

        El semáforo limita conexiones simultáneas:
        - Máximo 50 requests abiertas al mismo tiempo
        - Evita saturar el servidor o quedarnos sin file descriptors

        Args:
            session: Sesión de aiohttp
            order_id: ID del pedido
            url_base: URL base de la API
            headers: Headers con credenciales

        Returns:
            tuple: (order_id, nombre del seller)
        """
        url_detalle = f"{url_base}/{order_id}"

        # async with rate_limiter: espera si superamos 90 req/seg
        # async with semaphore: espera si hay 50 conexiones abiertas
        async with self._rate_limiter:
            async with self._semaphore:
                for intento in range(3):
                    try:
                        timeout = aiohttp.ClientTimeout(total=10)
                        async with session.get(url_detalle, headers=headers, timeout=timeout) as response:
                            # Si nos devuelve 429 (rate limited), esperar y reintentar
                            if response.status == 429:
                                retry_after = int(response.headers.get('Retry-After', 5))
                                logger.warning(f"Rate limited por VTEX, esperando {retry_after}s")
                                await asyncio.sleep(retry_after)
                                continue

                            if response.status == 200:
                                data = await response.json()
                                sellers = data.get("sellers", [])
                                if sellers:
                                    return order_id, sellers[0].get("name", "No encontrado")
                                return order_id, "Sin seller"
                            else:
                                logger.warning(f"Status {response.status} para pedido {order_id}")

                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout para pedido {order_id}, intento {intento + 1}/3")
                        await asyncio.sleep(2 ** intento)  # Backoff exponencial: 1s, 2s, 4s
                    except Exception as e:
                        logger.warning(f"Error al buscar seller para {order_id}: {e}")
                        await asyncio.sleep(2 ** intento)

        return order_id, "Error al obtener seller"

    async def obtener_todos_sellers(self, pedidos_unicos: dict, url_base, headers):
        """
        Obtiene sellers para todos los pedidos en paralelo con rate limiting.

        Flujo:
        1. Crea todas las tareas async (una por pedido)
        2. asyncio.as_completed las ejecuta respetando el rate limiter
        3. A medida que completan, asigna el seller al pedido (O(1) con dict)

        Args:
            pedidos_unicos: Dict {order_id: pedido_dict}
            url_base: URL base de la API
            headers: Headers con credenciales

        Returns:
            dict: pedidos_unicos con el campo 'seller' agregado
        """
        self._init_async_controls()

        # Configurar conector con límite de conexiones
        connector = aiohttp.TCPConnector(
            limit=self.MAX_CONCURRENT_CONNECTIONS,
            limit_per_host=self.MAX_CONCURRENT_CONNECTIONS
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            # Crear todas las tareas
            tasks = [
                self.buscar_seller_async(session, order_id, url_base, headers)
                for order_id in pedidos_unicos.keys()
            ]

            total = len(tasks)
            logger.info(f"Iniciando búsqueda de sellers para {total} pedidos...")

            # Procesar a medida que completan
            completados = 0
            for coro in asyncio.as_completed(tasks):
                order_id, seller = await coro

                # Asignación O(1) gracias al diccionario
                if order_id in pedidos_unicos:
                    pedidos_unicos[order_id]["seller"] = seller

                completados += 1
                # Log de progreso cada 500 pedidos
                if completados % 500 == 0:
                    logger.info(f"Progreso sellers: {completados}/{total} ({100*completados//total}%)")

            logger.info(f"Búsqueda de sellers completada: {completados}/{total}")

        return pedidos_unicos

    def descargarVtex(self, fecha_inicio_usuario, fecha_fin_usuario, credenciales, filtros=None):
        """
        Descarga pedidos de VTEX usando la API.

        Args:
            fecha_inicio_usuario: Fecha inicio DD/MM/YYYY
            fecha_fin_usuario: Fecha fin DD/MM/YYYY
            credenciales: Objeto UsuarioVtex con app_key, app_token, account_name
            filtros: Diccionario con filtros a aplicar (ej: {'estados': ['invoiced']})

        Returns:
            DataFrame: Pedidos de VTEX procesados
        """
        # Configuración API usando credenciales de la base de datos
        url = f"https://{credenciales.account_name}.vtexcommercestable.com.br/api/oms/pvt/orders"
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'X-VTEX-API-AppKey': credenciales.app_key,
            'X-VTEX-API-AppToken': credenciales.app_token
        }

        # Fechas de entrada
        fecha_desde = datetime.strptime(fecha_inicio_usuario, "%d/%m/%Y") + timedelta(hours=3)
        fecha_hasta = datetime.strptime(fecha_fin_usuario, "%d/%m/%Y") + timedelta(
            hours=23, minutes=59, seconds=59
        ) + timedelta(hours=3)

        per_page = 100
        todos_los_pedidos = []
        fecha_actual = fecha_desde
        delta = timedelta(days=1)

        # Descargar pedidos por intervalos
        while fecha_actual < fecha_hasta:
            fecha_siguiente = fecha_actual + delta
            if fecha_siguiente > fecha_hasta:
                fecha_siguiente = fecha_hasta

            pedidos, paginas = self.get_pedidos(fecha_actual, fecha_siguiente, url, headers, filtros)
            logger.info(f"Probando con {fecha_actual} a {fecha_siguiente} - {paginas} páginas")

            if paginas > 30:
                delta = delta / 2
                logger.info("Demasiadas páginas, achicando intervalo")
                continue

            # Si está bien, descargamos todas las páginas del subintervalo
            for page in range(1, paginas + 1):
                params = {
                    "f_creationDate": f"creationDate:[{self.formatear(fecha_actual)} TO {self.formatear(fecha_siguiente)}]",
                    "page": page,
                    "per_page": per_page,
                    "orderBy": "creationDate,asc"
                }

                # Aplicar filtro de estados si se proporcionó
                if filtros and filtros.get('estados'):
                    estados_str = ','.join(filtros['estados'])
                    params["f_status"] = estados_str

                response = requests.get(url, headers=headers, params=params)
                data = response.json()
                pedidos = data.get("list", [])
                todos_los_pedidos.extend(pedidos)
                logger.info(f"Página {page}/{paginas} del intervalo - {len(pedidos)} pedidos")

            fecha_actual = fecha_siguiente
            delta = timedelta(days=1)  # restauramos el paso si venía de achicarlo

        logger.info(f"Total pedidos descargados: {len(todos_los_pedidos)}")

        pedidos_unicos = {}
        for pedido in todos_los_pedidos:
            pedidos_unicos[pedido["orderId"]] = pedido

        pedidos_duplicados = len(todos_los_pedidos) - len(pedidos_unicos)
        logger.info(f"Pedidos únicos: {len(pedidos_unicos)} (eliminados {pedidos_duplicados} duplicados)")

        logger.info("Buscando seller de cada pedido (con rate limiting)...")
        pedidos_unicos = asyncio.run(
            self.obtener_todos_sellers(pedidos_unicos, url, headers)
        )

        # Convertir a DataFrame
        pedidos_vtex = pd.DataFrame(list(pedidos_unicos.values()))

        # Seleccionar solo las columnas necesarias (incluye totalValue para el valor del pedido)
        columnas_requeridas = ["orderId", "sequence", "creationDate", "paymentNames", "seller", "statusDescription", "totalValue"]
        columnas_disponibles = [col for col in columnas_requeridas if col in pedidos_vtex.columns]
        pedidos_vtex = pedidos_vtex[columnas_disponibles]

        # Exportar archivo final
        ruta_carpeta = os.path.join(self.ruta_carpeta, "vtex")
        os.makedirs(ruta_carpeta, exist_ok=True)

        archivo_final = os.path.join(
            ruta_carpeta,
            f"pedidos_vtex_{fecha_desde.date()}_a_{fecha_hasta.date()}.xlsx"
        )
        pedidos_vtex.to_excel(archivo_final, index=False)
        logger.info(f"Archivo final exportado a: {archivo_final}")

        return pedidos_vtex
