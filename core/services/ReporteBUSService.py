"""
Servicio para importar reportes BUS desde archivos Excel.
"""
from __future__ import annotations

from typing import BinaryIO

from core.models import ReporteBUS, TransaccionBUS

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ReporteBUSService:
    """Servicio para importar reportes de transacciones BUS desde Excel."""

    def importar_desde_excel(self, archivo: BinaryIO, reporte: ReporteBUS) -> int:
        """
        Importa transacciones desde un archivo Excel de BUS.

        Args:
            archivo: Archivo Excel subido (InMemoryUploadedFile o similar)
            reporte: Objeto ReporteBUS donde guardar las transacciones

        Returns:
            int: Cantidad de transacciones importadas

        Raises:
            ValueError: Si el archivo no tiene las columnas requeridas
        """
        logger.info(f"Importando transacciones BUS desde Excel para reporte #{reporte.id}")

        try:
            df = pd.read_excel(archivo)
            logger.info(f"Archivo leído: {len(df)} filas, columnas: {list(df.columns)}")

            columnas_requeridas = [
                'PEDIDO',
                'TIPO PEDIDO',
                'TIPO COMPROBANTE',
                'IMPORTE TOTAL',
                'FECHA RECEPCION DATOS'
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
                    pedido_raw = row.get('PEDIDO', '')
                    if pd.notna(pedido_raw):
                        if isinstance(pedido_raw, float) and pedido_raw.is_integer():
                            pedido_raw = int(pedido_raw)
                    pedido = str(pedido_raw).strip()

                    if not pedido or pedido == 'nan':
                        continue

                    tipo_pedido = str(row.get('TIPO PEDIDO', '')).strip()
                    tipo_comprobante = str(row.get('TIPO COMPROBANTE', '')).strip()

                    importe_raw = row.get('IMPORTE TOTAL', 0)
                    importe_total = 0
                    if pd.notna(importe_raw):
                        try:
                            importe_total = float(importe_raw)
                        except (ValueError, TypeError):
                            importe_total = 0

                    fecha_recepcion = str(row.get('FECHA RECEPCION DATOS', '')).strip()

                    transaccion = TransaccionBUS(
                        pedido=pedido,
                        tipo_pedido=tipo_pedido,
                        tipo_comprobante=tipo_comprobante,
                        importe_total=importe_total,
                        fecha_recepcion_datos=fecha_recepcion,
                        reporte=reporte
                    )
                    transacciones_objetos.append(transaccion)

                except Exception as e:
                    logger.warning(f"Error procesando fila: {e}")
                    continue

            if transacciones_objetos:
                TransaccionBUS.objects.bulk_create(
                    transacciones_objetos,
                    batch_size=1000
                )
                logger.info(f"Importadas {len(transacciones_objetos)} transacciones BUS")

            return len(transacciones_objetos)

        except Exception as e:
            logger.error(f"Error al importar Excel BUS: {str(e)}", exc_info=True)
            raise
