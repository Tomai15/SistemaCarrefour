from __future__ import annotations

import os.path
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from django.conf import settings
from django.db import models
from django.db.models import QuerySet, CharField, CASCADE
from django.utils.translation import gettext_lazy as _
import pandas as pd


# =============================================================================
# MODELOS DE USUARIOS/CREDENCIALES
# =============================================================================

class UsuarioPayway(models.Model):
    """Credenciales para acceder a la plataforma Payway."""

    usuario = models.CharField(max_length=100)
    clave = models.CharField(max_length=100)

    def __str__(self) -> str:
        return f"Credenciales Payway - {self.usuario}"


class UsuarioCDP(models.Model):
    """Credenciales para acceder a la plataforma CDP."""

    usuario = models.CharField(max_length=100)
    clave = models.CharField(max_length=100)

    def __str__(self) -> str:
        return f"Credenciales CDP - {self.usuario}"


class UsuarioVtex(models.Model):
    """Credenciales para acceder a la API de VTEX."""

    app_key = models.CharField(max_length=200, verbose_name="API App Key")
    app_token = models.CharField(max_length=500, verbose_name="API App Token")
    account_name = models.CharField(max_length=100, default="carrefourar", verbose_name="Account Name")

    def __str__(self) -> str:
        return f"Credenciales VTEX - {self.account_name}"


class UsuarioJanis(models.Model):
    """Credenciales para acceder a la API de Janis."""

    api_key = models.CharField(max_length=200, verbose_name="Janis API Key")
    api_secret = models.CharField(max_length=500, verbose_name="Janis API Secret")
    client_code = models.CharField(max_length=100, verbose_name="Janis Client Code")

    def __str__(self) -> str:
        return f"Credenciales Janis - {self.client_code}"

    class Meta:
        verbose_name = "Usuario Janis"
        verbose_name_plural = "Usuarios Janis"


# =============================================================================
# MODELOS DE FILTROS VTEX
# =============================================================================

class TipoFiltroVtex(models.Model):
    """
    Catálogo de tipos de filtros disponibles en la API de VTEX.

    Ejemplos: estado del pedido, método de pago, seller, etc.
    """
    codigo = models.CharField(
        max_length=50,
        unique=True,
        help_text="Código interno del filtro (ej: 'estado', 'metodo_pago')"
    )
    nombre = models.CharField(
        max_length=100,
        help_text="Nombre legible del filtro (ej: 'Estado del pedido')"
    )
    parametro_api = models.CharField(
        max_length=50,
        help_text="Parámetro que usa la API de VTEX (ej: 'f_status', 'f_paymentNames')"
    )
    activo = models.BooleanField(
        default=True,
        help_text="Si está activo, aparece como opción en el formulario"
    )

    class Meta:
        verbose_name = "Tipo de Filtro VTEX"
        verbose_name_plural = "Tipos de Filtros VTEX"
        ordering = ['nombre']

    def __str__(self) -> str:
        return self.nombre


class ValorFiltroVtex(models.Model):
    """
    Catálogo de valores posibles para cada tipo de filtro.

    Ejemplos para tipo 'estado': invoiced, canceled, payment-pending, etc.
    """
    tipo_filtro = models.ForeignKey(
        TipoFiltroVtex,
        on_delete=models.CASCADE,
        related_name='valores',
        help_text="Tipo de filtro al que pertenece este valor"
    )
    codigo = models.CharField(
        max_length=100,
        help_text="Código que usa la API de VTEX (ej: 'invoiced', 'payment-pending')"
    )
    nombre = models.CharField(
        max_length=100,
        help_text="Nombre legible del valor (ej: 'Facturado', 'Pago Pendiente')"
    )
    activo = models.BooleanField(
        default=True,
        help_text="Si está activo, aparece como opción en el formulario"
    )

    class Meta:
        verbose_name = "Valor de Filtro VTEX"
        verbose_name_plural = "Valores de Filtros VTEX"
        ordering = ['tipo_filtro', 'nombre']
        unique_together = ['tipo_filtro', 'codigo']

    def __str__(self) -> str:
        return f"{self.tipo_filtro.nombre}: {self.nombre}"


# =============================================================================
# MODELOS DE REPORTES
# =============================================================================

class ReportePayway(models.Model):
    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    estado = models.CharField(
        max_length=15,
        choices=Estado.choices,
        default=Estado.PENDIENTE
    )

    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()

    def generar_reporter_excel(self) -> str:
        """
        Genera el archivo Excel del reporte y retorna la ruta completa del archivo generado.

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'reporte_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')
        transacciones = self.transacciones.all()
        transacciones_convertidas = list(map(lambda transaccion: transaccion.convertir_en_diccionario(), transacciones))
        data_frame_transacciones = pd.DataFrame(transacciones_convertidas)
        if not data_frame_transacciones.empty and 'fecha' in data_frame_transacciones.columns:
            # Convertir UTC → hora Argentina, luego sacar timezone para que Excel lo muestre bien
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final,index=False)
        return ruta_final


class ReporteVtex(models.Model):
    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    estado = models.CharField(
        max_length=15,
        choices=Estado.choices,
        default=Estado.PENDIENTE
    )

    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()

    # DEPRECATED: Campo legacy, usar la relación filtros_aplicados en su lugar
    # Se mantiene temporalmente para migración de datos
    filtros = models.JSONField(
        null=True,
        blank=True,
        verbose_name='Filtros aplicados (LEGACY)',
        help_text='DEPRECATED: Usar filtros_aplicados'
    )

    incluir_sellers = models.BooleanField(
        default=True,
        verbose_name='Incluir sellers',
        help_text='Si está activo, busca el seller de cada pedido (proceso lento). Si está desactivado, el reporte se genera más rápido.'
    )

    class Meta:
        verbose_name = "Reporte VTEX"
        verbose_name_plural = "Reportes VTEX"

    def __str__(self) -> str:
        return f"Reporte VTEX #{self.id} ({self.fecha_inicio} - {self.fecha_fin})"

    def obtener_filtros_por_tipo(self, codigo_tipo: str) -> QuerySet[ValorFiltroVtex]:
        """
        Obtiene los valores de filtro aplicados para un tipo específico.

        Args:
            codigo_tipo: Código del tipo de filtro (ej: 'estado')

        Returns:
            QuerySet de ValorFiltroVtex
        """
        return ValorFiltroVtex.objects.filter(
            filtros_reportes__reporte=self,
            tipo_filtro__codigo=codigo_tipo
        )

    def obtener_filtros_para_api(self) -> dict[str, list[str]]:
        """
        Genera el diccionario de filtros para enviar a la API de VTEX.

        Returns:
            dict: {parametro_api: [valor1, valor2, ...]}
        """
        filtros_api: dict[str, list[str]] = {}
        for filtro in self.filtros_aplicados.select_related('tipo_filtro', 'valor_filtro').all():
            param = filtro.tipo_filtro.parametro_api
            if param not in filtros_api:
                filtros_api[param] = []
            filtros_api[param].append(filtro.valor_filtro.codigo)
        return filtros_api

    def generar_reporter_excel(self) -> str:
        """
        Genera el archivo Excel del reporte y retorna la ruta completa del archivo generado.

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'reporte_vtex_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')
        transacciones = self.transacciones.all()
        transacciones_convertidas = list(map(lambda transaccion: transaccion.convertir_en_diccionario(), transacciones))
        data_frame_transacciones = pd.DataFrame(transacciones_convertidas)
        if not data_frame_transacciones.empty and 'fecha' in data_frame_transacciones.columns:
            # Convertir UTC → hora Argentina, luego sacar timezone para que Excel lo muestre bien
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final,index=False)
        return ruta_final


class FiltroReporteVtex(models.Model):
    """
    Relación entre un ReporteVtex y los filtros aplicados.

    Permite asociar múltiples valores de filtro a un reporte.
    """
    reporte = models.ForeignKey(
        ReporteVtex,
        on_delete=models.CASCADE,
        related_name='filtros_aplicados',
        help_text="Reporte al que se aplica este filtro"
    )
    tipo_filtro = models.ForeignKey(
        TipoFiltroVtex,
        on_delete=models.CASCADE,
        related_name='filtros_reportes',
        help_text="Tipo de filtro aplicado"
    )
    valor_filtro = models.ForeignKey(
        ValorFiltroVtex,
        on_delete=models.CASCADE,
        related_name='filtros_reportes',
        help_text="Valor del filtro aplicado"
    )

    class Meta:
        verbose_name = "Filtro de Reporte VTEX"
        verbose_name_plural = "Filtros de Reportes VTEX"
        unique_together = ['reporte', 'tipo_filtro', 'valor_filtro']

    def __str__(self) -> str:
        return f"Reporte #{self.reporte.id} - {self.tipo_filtro.nombre}: {self.valor_filtro.nombre}"

    def clean(self) -> None:
        """Valida que el valor pertenezca al tipo de filtro correcto."""
        from django.core.exceptions import ValidationError
        if self.valor_filtro.tipo_filtro != self.tipo_filtro:
            raise ValidationError(
                f"El valor '{self.valor_filtro}' no pertenece al tipo de filtro '{self.tipo_filtro}'"
            )


class ReporteCDP(models.Model):
    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    estado = models.CharField(
        max_length=15,
        choices=Estado.choices,
        default=Estado.PENDIENTE
    )
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()

    def generar_reporter_excel(self) -> str:
        """
        Genera el archivo Excel del reporte y retorna la ruta completa del archivo generado.

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'reporte_cdp_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')
        transacciones = self.transacciones.all()
        transacciones_convertidas = list(map(lambda transaccion: transaccion.convertir_en_diccionario(), transacciones))
        data_frame_transacciones = pd.DataFrame(transacciones_convertidas)
        if not data_frame_transacciones.empty and 'fecha' in data_frame_transacciones.columns:
            # Convertir UTC → hora Argentina, luego sacar timezone para que Excel lo muestre bien
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final, index=False)
        return ruta_final

class ReporteJanis(models.Model):
    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    estado = models.CharField(
        max_length=15,
        choices=Estado.choices,
        default=Estado.PENDIENTE
    )
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()

    def generar_reporter_excel(self) -> str:
        """
        Genera el archivo Excel del reporte y retorna la ruta completa del archivo generado.

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'reporte_janis_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')
        transacciones = self.transacciones.all()
        transacciones_convertidas = list(map(lambda transaccion: transaccion.convertir_en_diccionario(), transacciones))
        data_frame_transacciones = pd.DataFrame(transacciones_convertidas)
        if not data_frame_transacciones.empty and 'fecha' in data_frame_transacciones.columns:
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        if not data_frame_transacciones.empty and 'fecha_entrega' in data_frame_transacciones.columns:
            data_frame_transacciones['fecha_entrega'] = data_frame_transacciones['fecha_entrega'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final, index=False)
        return ruta_final


class TransaccionJanis(models.Model):
    numero_pedido = models.CharField(max_length=100)
    numero_transaccion = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    fecha_entrega = models.DateTimeField(null=True, blank=True)
    medio_pago = models.CharField(max_length=100)
    seller = models.CharField(max_length=100)
    estado = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReporteJanis, on_delete=models.CASCADE, related_name='transacciones')
    estados_entregado: ClassVar[list[str]] = [
        "delivered", "inDelivery",
        "readyForDelivery", "readyForInternalDistribution", "en auditoria",
        "procesandoPromociones"
    ]

    def convertir_en_diccionario(self) -> dict[str, Any]:
        return {
            'Pedido': self.numero_pedido,
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado': self.estado,
            'fecha_entrega': self.fecha_entrega
        }

    def estado_entregado(self) -> bool:
        return any(estado_entregado in self.estado for estado_entregado in self.estados_entregado)


class Cruce(models.Model):
    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    estado = models.CharField(
        max_length=15,
        choices=Estado.choices,
        default=Estado.PENDIENTE
    )
    fecha_inicio = models.DateField()
    fecha_fin = models.DateField()
    fecha_realizado = models.DateField(null=True, blank=True)
    revisar = models.CharField(max_length=100, blank=True, default='')

    # Referencias a los reportes usados en el cruce
    reporte_vtex = models.ForeignKey(
        'ReporteVtex', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='cruces', verbose_name='Reporte VTEX'
    )
    reporte_payway = models.ForeignKey(
        'ReportePayway', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='cruces', verbose_name='Reporte Payway'
    )
    reporte_cdp = models.ForeignKey(
        'ReporteCDP', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='cruces', verbose_name='Reporte CDP'
    )
    reporte_janis = models.ForeignKey(
        'ReporteJanis', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='cruces', verbose_name='Reporte Janis'
    )

    def generar_reporter_excel(
        self,
        incluir_observaciones: bool = True,
        incluir_precio_payway: bool = False,
        incluir_precio_vtex: bool = False
    ) -> str:
        """
        Genera el archivo Excel del cruce.

        Args:
            incluir_observaciones: Si es True, incluye la columna resultado_cruce
            incluir_precio_payway: Si es True, incluye las columnas monto_payway y monto_payway_2
            incluir_precio_vtex: Si es True, incluye la columna valor_vtex

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'cruce_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')

        with pd.ExcelWriter(ruta_final, engine='openpyxl') as writer:
            # Hoja principal: Cruce
            transacciones = self.transacciones.all()
            transacciones_convertidas = list(map(
                lambda t: t.convertir_en_diccionario(
                    incluir_observaciones=incluir_observaciones,
                    incluir_precio_payway=incluir_precio_payway,
                    incluir_precio_vtex=incluir_precio_vtex
                ),
                transacciones
            ))
            df_cruce = pd.DataFrame(transacciones_convertidas)
            if not df_cruce.empty and 'fecha' in df_cruce.columns and pd.api.types.is_datetime64_any_dtype(df_cruce['fecha']):
                df_cruce['fecha'] = df_cruce['fecha'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
            if not df_cruce.empty and 'fecha_entrega' in df_cruce.columns and pd.api.types.is_datetime64_any_dtype(df_cruce['fecha_entrega']):
                df_cruce['fecha_entrega'] = df_cruce['fecha_entrega'].dt.tz_convert('America/Argentina/Buenos_Aires').dt.tz_localize(None)
            df_cruce.to_excel(writer, sheet_name='Cruce', index=False)
            """
            # Hoja VTEX (si existe)
            if self.reporte_vtex:
                transacciones_vtex = list(map(
                    lambda t: t.convertir_en_diccionario(),
                    self.reporte_vtex.transacciones.all()
                ))
                df_vtex = pd.DataFrame(transacciones_vtex)
                if not df_vtex.empty and 'fecha' in df_vtex.columns:
                    df_vtex['fecha'] = df_vtex['fecha'].dt.tz_localize(None)
                df_vtex.to_excel(writer, sheet_name='VTEX', index=False)

            # Hoja Payway (si existe)
            if self.reporte_payway:
                transacciones_payway = list(map(
                    lambda t: t.convertir_en_diccionario(),
                    self.reporte_payway.transacciones.all()
                ))
                df_payway = pd.DataFrame(transacciones_payway)
                if not df_payway.empty and 'fecha' in df_payway.columns:
                    df_payway['fecha'] = df_payway['fecha'].dt.tz_localize(None)
                df_payway.to_excel(writer, sheet_name='Payway', index=False)

            # Hoja CDP (si existe)
            if self.reporte_cdp:
                transacciones_cdp = list(map(
                    lambda t: t.convertir_en_diccionario(),
                    self.reporte_cdp.transacciones.all()
                ))
                df_cdp = pd.DataFrame(transacciones_cdp)
                if not df_cdp.empty and 'fecha' in df_cdp.columns:
                    df_cdp['fecha'] = df_cdp['fecha'].dt.tz_localize(None)
                df_cdp.to_excel(writer, sheet_name='CDP', index=False)

            # Hoja Janis (si existe)
            if self.reporte_janis:
                transacciones_janis = list(map(
                    lambda t: t.convertir_en_diccionario(),
                    self.reporte_janis.transacciones.all()
                ))
                df_janis = pd.DataFrame(transacciones_janis)
                if not df_janis.empty and 'fecha' in df_janis.columns:
                    df_janis['fecha'] = df_janis['fecha'].dt.tz_localize(None)
                df_janis.to_excel(writer, sheet_name='Janis', index=False)
            """
        return ruta_final


class TransaccionCruce(models.Model):
    numero_pedido = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField(null=True, blank=True)
    fecha_entrega = models.DateTimeField(null=True, blank=True)
    medio_pago = models.CharField(max_length=100, blank=True, default='')
    seller = models.CharField(max_length=100, blank=True, default='')
    estado_vtex = models.CharField(max_length=100, blank=True, default='')
    estado_payway = models.CharField(max_length=100, blank=True, default='')
    estado_payway_2 = models.CharField(max_length=100, blank=True, default='')
    estado_cdp = models.CharField(max_length=100, blank=True, default='')
    estado_janis = models.CharField(max_length=100, blank=True, default='')
    resultado_cruce = models.CharField(max_length=255, blank=True, default='')
    monto_payway = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    monto_payway_2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    valor_vtex = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    cruce = models.ForeignKey(Cruce, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(
        self,
        incluir_observaciones: bool = True,
        incluir_precio_payway: bool = False,
        incluir_precio_vtex: bool = False
    ) -> dict[str, Any]:
        datos: dict[str, Any] = {
            'Pedido': self.numero_pedido,
            'fecha': self.fecha_hora,
            'fecha_entrega': self.fecha_entrega,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado_vtex': self.estado_vtex,
            'estado_payway': self.estado_payway,
            'estado_payway_2': self.estado_payway_2,
            'estado_cdp': self.estado_cdp,
            'estado_janis': self.estado_janis,
        }
        if incluir_precio_vtex:
            datos['valor_vtex'] = self.valor_vtex
        if incluir_precio_payway:
            datos['monto_payway'] = self.monto_payway
            datos['monto_payway_2'] = self.monto_payway_2
        if incluir_observaciones:
            datos['resultado_cruce'] = self.resultado_cruce
        return datos


class TransaccionCDP(models.Model):
    numero_pedido = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    numero_tienda = models.DecimalField(max_digits=10, decimal_places=2)
    estado = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReporteCDP, on_delete=models.CASCADE, related_name='transacciones')
    estados_entregados: ClassVar[list[str]] = [
        "finalizado", "disponible en drive", "disponible en sucursal",
        "disponible en sede", "pendiente de despacho", "pendiente de de envio a pup",
        "recepcion pendiente"
    ]

    def convertir_en_diccionario(self) -> dict[str, Any]:
        return {
            'Pedido': self.numero_pedido,
            'fecha': self.fecha_hora,
            'numero_tienda': self.numero_tienda,
            'estado': self.estado
        }

    def estado_entregado(self) -> bool:
        return any(keyword.lower() in self.estado.lower() for keyword in self.estados_entregados)




class TransaccionPayway(models.Model):
    numero_transaccion = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    monto = models.DecimalField(max_digits=10, decimal_places=2)
    estado = models.CharField(max_length=100)
    tarjeta = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReportePayway, on_delete=models.CASCADE, related_name='transacciones')
    estados_no_entregados: ClassVar[list[str]] = ["Pre autorizada", "Vencida"]

    def convertir_en_diccionario(self) -> dict[str, Any]:
        return {
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'monto': self.monto,
            'estado': self.estado,
            'tarjeta': self.tarjeta
        }

    def estado_no_cobrado(self) -> bool:
        return any(keyword in self.estado for keyword in self.estados_no_entregados)

class TransaccionVtex(models.Model):
    numero_pedido = models.CharField(max_length=100)
    numero_transaccion = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    medio_pago = models.CharField(max_length=100)
    seller = models.CharField(max_length=100)
    estado = models.CharField(max_length=100)
    valor = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name='Valor del pedido'
    )
    KEYWORDS_FOOD: ClassVar[list[str]] = ["carrefour", "hiper", "maxi", "market", "express", "trelew"]
    reporte = models.ForeignKey(ReporteVtex, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self) -> dict[str, Any]:
        return {
            'Pedido': self.numero_pedido,
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado': self.estado,
            'valor': self.valor
        }

    def pedido_electro(self) -> bool:
        return self.seller == "Hogar & Electro"

    def pedido_food(self) -> bool:
        return any(keyword.lower() in self.seller.lower() for keyword in self.KEYWORDS_FOOD)

    def pedido_marketplace(self) -> bool:
        return not self.pedido_electro() and not self.pedido_food()


# =============================================================================
# MODELOS DE CATALOGACION
# =============================================================================

class UsuarioCarrefourWeb(models.Model):
    """Credenciales para login en carrefour.com.ar via Playwright."""

    email = models.CharField(max_length=200)
    clave = models.CharField(max_length=200)

    class Meta:
        verbose_name = "Usuario Carrefour Web"
        verbose_name_plural = "Usuarios Carrefour Web"

    def __str__(self) -> str:
        return f"Credenciales Carrefour - {self.email}"


class TareaCatalogacion(models.Model):
    """Trackea ejecuciones de tareas de catalogacion (busqueda EANs, categorias, sellers, etc.)."""

    class Estado(models.TextChoices):
        PENDIENTE = 'PENDIENTE', _('Pendiente')
        PROCESANDO = 'PROCESANDO', _('Procesando')
        COMPLETADO = 'COMPLETADO', _('Completado')
        ERROR = 'ERROR', _('Error')

    class TipoTarea(models.TextChoices):
        BUSQUEDA_EANS = 'BUSQUEDA_EANS', _('Busqueda EANs')
        BUSQUEDA_CATEGORIAS = 'BUSQUEDA_CATEGORIAS', _('Busqueda Categorias')
        SELLERS_EXTERNOS = 'SELLERS_EXTERNOS', _('Sellers Externos')
        SELLERS_NO_CARREFOUR = 'SELLERS_NO_CARREFOUR', _('Sellers No Carrefour')
        ACTUALIZAR_MODAL = 'ACTUALIZAR_MODAL', _('Actualizar Modal')
        CONSULTA_VISIBILIDAD = 'CONSULTA_VISIBILIDAD', _('Consulta Visibilidad')
        EXPORT_CATALOGO = 'EXPORT_CATALOGO', _('Export Catalogo')
        CARGA_STOCK = 'CARGA_STOCK', _('Carga Stock')

    tipo = models.CharField(max_length=30, choices=TipoTarea.choices)
    estado = models.CharField(max_length=15, choices=Estado.choices, default=Estado.PENDIENTE)
    fecha_creacion = models.DateTimeField(auto_now_add=True)
    logs = models.TextField(blank=True, default='')
    progreso_actual = models.IntegerField(default=0)
    progreso_total = models.IntegerField(default=0)
    archivo_resultado = models.FileField(upload_to='catalogacion/', null=True, blank=True)

    class Meta:
        verbose_name = "Tarea de Catalogacion"
        verbose_name_plural = "Tareas de Catalogacion"
        ordering = ['-id']

    def __str__(self) -> str:
        return f"Tarea #{self.id} - {self.get_tipo_display()} ({self.get_estado_display()})"

    def agregar_log(self, mensaje: str) -> None:
        """Agrega un mensaje al log y guarda en DB."""
        if self.logs:
            self.logs += f"\n{mensaje}"
        else:
            self.logs = mensaje
        self.save(update_fields=['logs'])


class SellerVtex(models.Model):
    """Credenciales y datos de un seller en VTEX."""

    nombre = models.CharField(max_length=200)
    url = models.CharField(max_length=300)
    app_key = models.CharField(max_length=200, verbose_name="API App Key")
    app_token = models.CharField(max_length=500, verbose_name="API App Token")
    account_name = models.CharField(max_length=100, verbose_name="Account Name")
    marketplace = models.ForeignKey(
        'self', null=True, blank=True, on_delete=models.SET_NULL,
        related_name='sellers', verbose_name="Marketplace madre",
        help_text="Seller madre/marketplace. Dejar vacio si este seller ES el marketplace."
    )

    class Meta:
        verbose_name = "Seller VTEX"
        verbose_name_plural = "Sellers VTEX"

    def __str__(self) -> str:
        return self.nombre


class ProductoVtex(models.Model):
    productId = models.CharField(max_length=200, unique=True)


class SkuVtex(models.Model):
    skuId = models.CharField(max_length=200, unique=True)
    producto = models.ForeignKey(ProductoVtex, on_delete=models.CASCADE, related_name='skus')


class ConsultaVisibilidad(models.Model):
    sku = models.ForeignKey(SkuVtex, on_delete=models.CASCADE, related_name='consultas_visibilidad')
    seller = models.ForeignKey(SellerVtex, on_delete=models.CASCADE, related_name='consultas_visibilidad')
    tarea = models.ForeignKey(TareaCatalogacion, on_delete=models.CASCADE, related_name='consultas_visibilidad', null=True, blank=True)
    visible = models.BooleanField()
    motivo = models.CharField(max_length=300, blank=True, default='')
    stock = models.DecimalField(max_digits=10, decimal_places=0, null=True, blank=True)
    precio = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    tiene_imagenes = models.BooleanField(null=True, blank=True)
    fecha = models.DateTimeField(auto_now_add=True)