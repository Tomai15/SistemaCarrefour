import os.path

from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _
import pandas as pd


# Create your models here.

class UsuarioPayway(models.Model):
    usuario = models.CharField(max_length=100)
    clave = models.CharField(max_length=100)

    def __str__(self):
        return f"Credenciales Payway - {self.usuario}"


class UsuarioCDP(models.Model):
    usuario = models.CharField(max_length=100)
    clave = models.CharField(max_length=100)

    def __str__(self):
        return f"Credenciales CDP - {self.usuario}"


class UsuarioVtex(models.Model):
    app_key = models.CharField(max_length=200, verbose_name="API App Key")
    app_token = models.CharField(max_length=500, verbose_name="API App Token")
    account_name = models.CharField(max_length=100, default="carrefourar", verbose_name="Account Name")

    def __str__(self):
        return f"Credenciales VTEX - {self.account_name}"


class UsuarioJanis(models.Model):
    api_key = models.CharField(max_length=200, verbose_name="Janis API Key")
    api_secret = models.CharField(max_length=500, verbose_name="Janis API Secret")
    client_code = models.CharField(max_length=100, verbose_name="Janis Client Code")

    def __str__(self):
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

    def __str__(self):
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

    def __str__(self):
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

    def generar_reporter_excel(self):
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
            # .dt accedde a las propiedades de fecha de la serie
            # .tz_localize(None) elimina la información de zona horaria (lo hace "naive")
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_localize(None)
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

    class Meta:
        verbose_name = "Reporte VTEX"
        verbose_name_plural = "Reportes VTEX"

    def __str__(self):
        return f"Reporte VTEX #{self.id} ({self.fecha_inicio} - {self.fecha_fin})"

    def obtener_filtros_por_tipo(self, codigo_tipo):
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

    def obtener_filtros_para_api(self):
        """
        Genera el diccionario de filtros para enviar a la API de VTEX.

        Returns:
            dict: {parametro_api: [valor1, valor2, ...]}
        """
        filtros_api = {}
        for filtro in self.filtros_aplicados.select_related('tipo_filtro', 'valor_filtro').all():
            param = filtro.tipo_filtro.parametro_api
            if param not in filtros_api:
                filtros_api[param] = []
            filtros_api[param].append(filtro.valor_filtro.codigo)
        return filtros_api

    def generar_reporter_excel(self):
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
            # .dt accedde a las propiedades de fecha de la serie
            # .tz_localize(None) elimina la información de zona horaria (lo hace "naive")
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_localize(None)
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

    def __str__(self):
        return f"Reporte #{self.reporte.id} - {self.tipo_filtro.nombre}: {self.valor_filtro.nombre}"

    def clean(self):
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

    def generar_reporter_excel(self):
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
            # .dt accedde a las propiedades de fecha de la serie
            # .tz_localize(None) elimina la información de zona horaria (lo hace "naive")
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_localize(None)
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

    def generar_reporter_excel(self):
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
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final, index=False)
        return ruta_final


class TransaccionJanis(models.Model):
    numero_pedido = models.CharField(max_length=100)
    numero_transaccion = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    medio_pago = models.CharField(max_length=100)
    seller = models.CharField(max_length=100)
    estado = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReporteJanis, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {
            'Pedido': self.numero_pedido,
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado': self.estado
        }


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

    def generar_reporter_excel(self):
        """
        Genera el archivo Excel del cruce con múltiples hojas:
        - Cruce: Resultado del cruce de transacciones
        - VTEX: Reporte VTEX completo (si existe)
        - Payway: Reporte Payway completo (si existe)
        - CDP: Reporte CDP completo (si existe)
        - Janis: Reporte Janis completo (si existe)

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'cruce_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')

        with pd.ExcelWriter(ruta_final, engine='openpyxl') as writer:
            # Hoja principal: Cruce
            transacciones = self.transacciones.all()
            transacciones_convertidas = list(map(lambda t: t.convertir_en_diccionario(), transacciones))
            df_cruce = pd.DataFrame(transacciones_convertidas)
            if not df_cruce.empty and 'fecha' in df_cruce.columns:
                df_cruce['fecha'] = df_cruce['fecha'].dt.tz_localize(None)
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
    medio_pago = models.CharField(max_length=100, blank=True, default='')
    seller = models.CharField(max_length=100, blank=True, default='')
    estado_vtex = models.CharField(max_length=100, blank=True, default='')
    estado_payway = models.CharField(max_length=100, blank=True, default='')
    estado_payway_2 = models.CharField(max_length=100, blank=True, default='')
    estado_cdp = models.CharField(max_length=100, blank=True, default='')
    estado_janis = models.CharField(max_length=100, blank=True, default='')
    cruce = models.ForeignKey(Cruce, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {
            'Pedido': self.numero_pedido,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado_vtex': self.estado_vtex,
            'estado_payway': self.estado_payway,
            'estado_payway_2': self.estado_payway_2,
            'estado_cdp': self.estado_cdp,
            'estado_janis': self.estado_janis
        }


class TransaccionCDP(models.Model):
    numero_pedido = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    numero_tienda = models.DecimalField(max_digits=10, decimal_places=2)
    estado = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReporteCDP, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {
            'Pedido': self.numero_pedido,
            'fecha': self.fecha_hora,
            'numero_tienda': self.numero_tienda,
            'estado': self.estado
        }




class TransaccionPayway(models.Model):
    numero_transaccion = models.CharField(max_length=100)
    fecha_hora = models.DateTimeField()
    monto = models.DecimalField(max_digits=10, decimal_places=2)
    estado = models.CharField(max_length=100)
    tarjeta = models.CharField(max_length=100)
    reporte = models.ForeignKey(ReportePayway, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {'Transaccion': self.numero_transaccion, 'fecha': self.fecha_hora,
                'monto': self.monto, 'estado': self.estado, 'tarjeta': self.tarjeta}


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
    reporte = models.ForeignKey(ReporteVtex, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {
            'Pedido': self.numero_pedido,
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado': self.estado,
            'valor': self.valor
        }