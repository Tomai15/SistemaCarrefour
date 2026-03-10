"""
Servicio para importar reportes de MercadoPago desde archivos Excel.
"""
from __future__ import annotations

from typing import BinaryIO

from core.models import ReporteMercadoPago, TransaccionMercadoPago

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ReporteMercadoPagoService:
    """Servicio para importar reportes de transacciones de MercadoPago desde Excel."""

    def importar_desde_excel(self, archivo: BinaryIO, reporte: ReporteMercadoPago) -> int:
        """
        Importa transacciones desde un archivo Excel de MercadoPago.

        Args:
            archivo: Archivo Excel subido (InMemoryUploadedFile o similar)
            reporte: Objeto ReporteMercadoPago donde guardar las transacciones

        Returns:
            int: Cantidad de transacciones importadas

        Raises:
            ValueError: Si el archivo no tiene las columnas requeridas
        """
        logger.info(f"Importando transacciones MercadoPago desde Excel para reporte #{reporte.id}")

        try:
            df = pd.read_excel(archivo)
            logger.info(f"Archivo leído: {len(df)} filas, columnas: {list(df.columns)}")

            columnas_requeridas = [
                'NÚMERO DE IDENTIFICACIÓN',
                'ID DE OPERACIÓN EN MERCADO PAGO',
                'FECHA DE ORIGEN',
                'VALOR DE LA COMPRA',
                'TIPO DE OPERACIÓN'
            ]

            columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
            if columnas_faltantes:
                raise ValueError(
                    f"El archivo no tiene las columnas requeridas: {columnas_faltantes}. "
                    f"Columnas encontradas: {list(df.columns)}"
                )

            transacciones_objetos = []

            for _, row in df.iterrows():
                try:
                    fecha_hora = pd.to_datetime(row['FECHA DE ORIGEN'])
                    if pd.isna(fecha_hora):
                        continue

                    numero_identificacion = row.get('NÚMERO DE IDENTIFICACIÓN', '')
                    if pd.notna(numero_identificacion):
                        if isinstance(numero_identificacion, float) and numero_identificacion.is_integer():
                            numero_identificacion = int(numero_identificacion)
                    numero_identificacion = str(numero_identificacion).strip()

                    id_operacion = row.get('ID DE OPERACIÓN EN MERCADO PAGO', '')
                    if pd.notna(id_operacion):
                        if isinstance(id_operacion, float) and id_operacion.is_integer():
                            id_operacion = int(id_operacion)
                    id_operacion = str(id_operacion).strip()

                    monto_raw = row.get('VALOR DE LA COMPRA', 0)
                    monto = 0
                    if pd.notna(monto_raw):
                        try:
                            monto = float(monto_raw)
                        except (ValueError, TypeError):
                            monto = 0

                    transaccion = TransaccionMercadoPago(
                        numero_identificacion=numero_identificacion,
                        id_operacion_mercado_pago=id_operacion,
                        fecha_hora=fecha_hora,
                        monto=monto,
                        tipo_operacion=str(row.get('TIPO DE OPERACIÓN', '')).strip(),
                        reporte=reporte
                    )
                    transacciones_objetos.append(transaccion)

                except Exception as e:
                    logger.warning(f"Error procesando fila: {e}")
                    continue

            if transacciones_objetos:
                TransaccionMercadoPago.objects.bulk_create(
                    transacciones_objetos,
                    batch_size=1000
                )
                logger.info(f"Importadas {len(transacciones_objetos)} transacciones MercadoPago")

            return len(transacciones_objetos)

        except Exception as e:
            logger.error(f"Error al importar Excel MercadoPago: {str(e)}", exc_info=True)
            raise
