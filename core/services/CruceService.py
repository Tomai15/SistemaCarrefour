from __future__ import annotations

from asgiref.sync import sync_to_async
from datetime import date
from typing import Any

from core.models import (
    Cruce, TransaccionCruce,
    ReporteVtex, ReportePayway, ReporteCDP, ReporteJanis,
    TransaccionVtex, TransaccionPayway, TransaccionCDP, TransaccionJanis
)

from django.conf import settings
import logging
import os

logger = logging.getLogger(__name__)


class CruceService:
    """Servicio para generar cruces de transacciones entre diferentes sistemas."""

    def __init__(self) -> None:
        """Inicializa el servicio de cruces."""
        pass

    async def generar_cruce(
        self,
        cruce_id: int,
        reporte_vtex_id: int | None = None,
        reporte_payway_id: int | None = None,
        reporte_cdp_id: int | None = None,
        reporte_janis_id: int | None = None
    ) -> bool:
        """
        Genera un cruce de transacciones entre los reportes seleccionados.

        Args:
            cruce_id: ID del objeto Cruce en la base de datos
            reporte_vtex_id: ID del reporte VTEX (opcional)
            reporte_payway_id: ID del reporte Payway (opcional)
            reporte_cdp_id: ID del reporte CDP (opcional)
            reporte_janis_id: ID del reporte Janis (opcional)

        Returns:
            bool: True si se genero exitosamente, False en caso contrario
        """
        try:
            # Obtener el cruce de la base de datos
            cruce = await sync_to_async(Cruce.objects.get)(id=cruce_id)

            # Actualizar estado a PROCESANDO
            cruce.estado = Cruce.Estado.PROCESANDO
            await sync_to_async(cruce.save)()

            logger.info(f"Generando cruce #{cruce_id}")

            # Obtener los reportes seleccionados
            reporte_vtex = None
            reporte_payway = None
            reporte_cdp = None
            reporte_janis = None

            if reporte_vtex_id:
                reporte_vtex = await sync_to_async(ReporteVtex.objects.get)(id=reporte_vtex_id)
            if reporte_payway_id:
                reporte_payway = await sync_to_async(ReportePayway.objects.get)(id=reporte_payway_id)
            if reporte_cdp_id:
                reporte_cdp = await sync_to_async(ReporteCDP.objects.get)(id=reporte_cdp_id)
            if reporte_janis_id:
                reporte_janis = await sync_to_async(ReporteJanis.objects.get)(id=reporte_janis_id)

            # Obtener las transacciones de cada reporte
            transacciones_vtex = []
            transacciones_payway = []
            transacciones_cdp = []
            transacciones_janis = []

            if reporte_vtex:
                transacciones_vtex = await sync_to_async(list)(
                    reporte_vtex.transacciones.all()
                )
            if reporte_payway:
                transacciones_payway = await sync_to_async(list)(
                    reporte_payway.transacciones.all()
                )
            if reporte_cdp:
                transacciones_cdp = await sync_to_async(list)(
                    reporte_cdp.transacciones.all()
                )
            if reporte_janis:
                transacciones_janis = await sync_to_async(list)(
                    reporte_janis.transacciones.all()
                )

            logger.info(
                f"Transacciones obtenidas - VTEX: {len(transacciones_vtex)}, "
                f"Payway: {len(transacciones_payway)}, CDP: {len(transacciones_cdp)}, "
                f"Janis: {len(transacciones_janis)}"
            )


            # Aqui deberas implementar tu propia logica para cruzar las transacciones
            # El metodo debe retornar una lista de diccionarios con la estructura esperada
            transacciones_cruzadas = await self.cruzar_transacciones(
                transacciones_vtex,
                transacciones_payway,
                transacciones_cdp,
                transacciones_janis
            )

            # Guardar transacciones cruzadas en la base de datos
            cantidad = await self.guardar_transacciones_cruce(transacciones_cruzadas, cruce)

            # Actualizar estado a COMPLETADO y fecha_realizado
            cruce.estado = Cruce.Estado.COMPLETADO
            cruce.fecha_realizado = date.today()
            await sync_to_async(cruce.save)()

            logger.info(
                f"Cruce #{cruce_id} generado exitosamente. "
                f"{cantidad} transacciones cruzadas guardadas."
            )
            return True

        except Cruce.DoesNotExist:
            logger.error(f"Cruce #{cruce_id} no encontrado")
            return False
        except Exception as e:
            logger.error(f"Error al generar cruce #{cruce_id}: {str(e)}", exc_info=True)
            try:
                cruce.estado = Cruce.Estado.ERROR
                await sync_to_async(cruce.save)()
            except:
                pass
            return False

    async def cruzar_transacciones(
        self,
        transacciones_vtex: list[TransaccionVtex],
        transacciones_payway: list[TransaccionPayway],
        transacciones_cdp: list[TransaccionCDP],
        transacciones_janis: list[TransaccionJanis]
    ) -> list[dict[str, Any]]:
        """
        Cruza las transacciones de los diferentes sistemas.

        Args:
            transacciones_vtex: Lista de TransaccionVtex
            transacciones_payway: Lista de TransaccionPayway
            transacciones_cdp: Lista de TransaccionCDP
            transacciones_janis: Lista de TransaccionJanis
        Returns:
            list: Lista de diccionarios con las transacciones cruzadas.
                  Cada diccionario debe tener las keys:
                  - numero_pedido
                  - fecha_hora (opcional)
                  - medio_pago (opcional)
                  - seller (opcional)
                  - estado_vtex (opcional)
                  - estado_payway (opcional)
                  - estado_cdp (opcional)
                  - estado_janis (opcional)
                  - resultado_cruce (opcional)
        """
        # Crear diccionarios indexados
        vtex_por_pedido = {t.numero_pedido: t for t in transacciones_vtex}
        cdp_por_pedido = {t.numero_pedido: t for t in transacciones_cdp}
        payway_por_transaccion = {t.numero_transaccion: t for t in transacciones_payway}

        # Ejemplo: indexar por numero_pedido
        janis_por_pedido = {t.numero_pedido: t for t in transacciones_janis}

        # DEBUG: Mostrar ejemplos de claves para cada diccionario
        logger.info("=" * 60)
        logger.info("DEBUG: Análisis de claves para cruce")
        logger.info("=" * 60)

        # Mostrar primeras 5 claves de VTEX
        vtex_keys = list(vtex_por_pedido.keys())[:5]
        logger.info(f"VTEX - Total pedidos: {len(vtex_por_pedido)}")
        logger.info(f"VTEX - Ejemplo claves (numero_pedido): {vtex_keys}")

        # Mostrar primeras 5 claves de CDP
        cdp_keys = list(cdp_por_pedido.keys())[:5]
        logger.info(f"CDP - Total pedidos: {len(cdp_por_pedido)}")
        logger.info(f"CDP - Ejemplo claves (numero_pedido): {cdp_keys}")

        # Mostrar primeras 5 claves de Payway
        payway_keys = list(payway_por_transaccion.keys())[:5]
        logger.info(f"PAYWAY - Total transacciones: {len(payway_por_transaccion)}")
        logger.info(f"PAYWAY - Ejemplo claves (numero_transaccion): {payway_keys}")

        # Mostrar primeras 5 claves de Janis
        janis_keys = list(janis_por_pedido.keys())[:5]
        logger.info(f"JANIS - Total pedidos: {len(janis_por_pedido)}")
        logger.info(f"JANIS - Ejemplo claves (numero_pedido): {janis_keys}")

        # Mostrar ejemplo de numero_transaccion de VTEX para comparar
        if transacciones_vtex:
            vtex_transacciones = [t.numero_transaccion for t in transacciones_vtex[:5]]
            logger.info(f"VTEX - Ejemplo numero_transaccion: {vtex_transacciones}")

        # Obtener todos los numeros de pedido unicos
        todos_los_pedidos = set(vtex_por_pedido.keys())
        logger.info(f"Total pedidos únicos a procesar: {len(todos_los_pedidos)}")

        transacciones_cruzadas = []
        matches_payway = 0
        matches_cdp = 0
        matches_janis = 0

        for pedido in todos_los_pedidos:
            vtex = vtex_por_pedido.get(pedido)
            cdp = cdp_por_pedido.get(pedido.split('-')[0])
            janis = janis_por_pedido.get(pedido)
            # Intentar match con Payway (buscar -1 y -2)
            payway = None
            payway2 = None
            pedido_convertido = self.convertir_pedido_transaccion_payway(pedido)

            if payway_por_transaccion.get(pedido_convertido):
                payway = payway_por_transaccion.get(pedido_convertido)
                # Buscar la segunda transacción (-2)
                pedido_convertido_2 = pedido_convertido.replace('-1', '-2')
                payway2 = payway_por_transaccion.get(pedido_convertido_2)
                matches_payway += 1
            elif vtex and payway_por_transaccion.get(vtex.numero_transaccion):
                payway = payway_por_transaccion.get(vtex.numero_transaccion)
                # Buscar la segunda transacción (-2)
                transaccion_2 = vtex.numero_transaccion.replace('-1', '-2')
                payway2 = payway_por_transaccion.get(transaccion_2)
                matches_payway += 1

            if cdp:
                matches_cdp += 1


            if janis:
                matches_janis += 1

            resultado_cruce = self.calcular_resultado_cruce(vtex, payway, cdp, janis)
            transacciones_cruzadas.append({
                'numero_pedido': pedido,
                'fecha_hora': vtex.fecha_hora if vtex else None,
                'fecha_entrega_janis': janis.fecha_entrega if janis else None,
                'medio_pago': vtex.medio_pago if vtex else 'N/A',
                'seller': vtex.seller if vtex else 'N/A',
                'estado_vtex': vtex.estado if vtex else 'N/A',
                'estado_payway': payway.estado if payway else 'N/A',
                'estado_payway_2': payway2.estado if payway2 else 'N/A',
                'estado_cdp': cdp.estado if cdp else 'N/A',
                'estado_janis': janis.estado if janis else 'N/A',
                'resultado_cruce': resultado_cruce,
                'monto_payway': payway.monto if payway else None,
                'monto_payway_2': payway2.monto if payway2 else None,
                'valor_vtex': vtex.valor if vtex else None,
            })

        # DEBUG: Resumen de matches
        logger.info("=" * 60)
        logger.info("DEBUG: Resumen de matches")
        logger.info("=" * 60)
        logger.info(f"Total transacciones cruzadas: {len(transacciones_cruzadas)}")
        logger.info(f"Matches con Payway: {matches_payway} ({100*matches_payway/len(transacciones_cruzadas) if transacciones_cruzadas else 0:.1f}%)")
        logger.info(f"Matches con CDP: {matches_cdp} ({100*matches_cdp/len(transacciones_cruzadas) if transacciones_cruzadas else 0:.1f}%)")
        logger.info(f"Matches con Janis: {matches_janis} ({100*matches_janis/len(transacciones_cruzadas) if transacciones_cruzadas else 0:.1f}%)")

        # DEBUG: Mostrar ejemplo de conversión de pedido a transacción payway
        if vtex_keys:
            ejemplo_pedido = vtex_keys[0]
            ejemplo_convertido = self.convertir_pedido_transaccion_payway(ejemplo_pedido)
            logger.info(f"Ejemplo conversión: '{ejemplo_pedido}' -> '{ejemplo_convertido}'")
            logger.info(f"¿Existe en Payway? {ejemplo_convertido in payway_por_transaccion}")

        return transacciones_cruzadas

    def convertir_pedido_transaccion_payway(self, pedido_vtex: str) -> str:
        partes = pedido_vtex.split('-')

        transaccion_payway = f"{partes[0]}-{int(partes[1])}"
        return transaccion_payway

    async def guardar_transacciones_cruce(
        self,
        transacciones: list[dict[str, Any]],
        cruce: Cruce
    ) -> int:
        """
        Guarda las transacciones cruzadas en la base de datos.

        Args:
            transacciones: Lista de diccionarios con las transacciones cruzadas
            cruce: Objeto Cruce

        Returns:
            int: Cantidad de transacciones guardadas
        """
        if not transacciones:
            logger.warning("Lista de transacciones cruzadas vacia, no hay nada que guardar")
            return 0

        transacciones_objetos = []

        for row in transacciones:
            try:
                transaccion = TransaccionCruce(
                    numero_pedido=str(row['numero_pedido']),
                    fecha_hora=row.get('fecha_hora'),
                    fecha_entrega=row.get('fecha_entrega_janis'),
                    medio_pago=str(row.get('medio_pago', '')),
                    seller=str(row.get('seller', '')),
                    estado_vtex=str(row.get('estado_vtex', '')),
                    estado_payway=str(row.get('estado_payway', '')),
                    estado_payway_2=str(row.get('estado_payway_2', '')),
                    estado_cdp=str(row.get('estado_cdp', '')),
                    estado_janis=str(row.get('estado_janis', '')),
                    resultado_cruce=str(row.get('resultado_cruce', '')),
                    monto_payway=row.get('monto_payway'),
                    monto_payway_2=row.get('monto_payway_2'),
                    valor_vtex=row.get('valor_vtex'),
                    cruce=cruce
                )
                transacciones_objetos.append(transaccion)

            except Exception as e:
                logger.warning(f"Error procesando transaccion cruzada {row.get('numero_pedido', 'N/A')}: {e}")
                continue

        # Insercion en lote
        if transacciones_objetos:
            await sync_to_async(TransaccionCruce.objects.bulk_create)(
                transacciones_objetos,
                batch_size=1000
            )
            logger.info(f"Guardadas {len(transacciones_objetos)} transacciones cruzadas")

        return len(transacciones_objetos)

    def calcular_resultado_cruce(
        self,
        vtex: TransaccionVtex | None,
        payway: TransaccionPayway | None,
        cdp: TransaccionCDP | None,
        janis: TransaccionJanis | None
    ) -> str:
        if not vtex:
            return ""

        if vtex.estado == "Verificando Fatura":
            if payway and payway.estado == "Pre autorizada":
                return ("Cobrar manualmente desde Payway, estado veri"
                        "ficando factura en vtex")
            else:
                    return ("Levantar ticket a WebCenter"
                            "pedido no existe en decidir")

        # Helper para verificar estado entregado de forma segura
        cdp_entregado = cdp.estado_entregado() if cdp else False
        janis_entregado = janis.estado_entregado() if janis else False
        payway_no_cobrado = payway.estado_no_cobrado() if payway else False
        cdp_anulado = cdp and cdp.estado == "Anulado sin factura"
        janis_cancelado = janis and janis.estado == "canceled"

        if vtex.medio_pago and "MercadoPagoPro" in vtex.medio_pago and not (vtex.pedido_electro() or vtex.pedido_marketplace()):
            # Logica de cruce MercadoPago
            if cdp_entregado or janis_entregado:
                if vtex.estado != "Faturado":
                    return "Verificar, entregado pero no facturado"

            elif cdp_anulado or janis_cancelado:
                if vtex.estado == "Pagamento Aprovado":
                    return "Verificar, anulado pero no cancelado en vtex"

            return ""

        elif vtex.pedido_food():
            # Logica de cruce Food
            if cdp_entregado or janis_entregado:
                if vtex.estado != "Faturado":
                    return "Verificar, entregado pero no facturado"
                elif payway_no_cobrado:
                    return "Verificar, no cobrado en Payway"
            elif cdp_anulado or janis_cancelado:
                if vtex.estado == "Pagamento Aprovado":
                    return "Verificar, anulado pero no cancelado en vtex"
                elif payway and payway.estado == "Pre autorizada":
                    return "Verificar, anulado pero preautorizado en payway"

            return ""

        elif vtex.pedido_electro():
            if payway_no_cobrado:
                return "Verificar, no cobrado en Payway"
            elif vtex.estado != "Faturado" and vtex.estado != "Cancelado":
                return "Verificar, no facturado"

        elif vtex.pedido_marketplace():
            if vtex.estado != "Faturado":
                return "Avisar a marketplace"

        return ""
