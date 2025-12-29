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

    def generar_reporter_excel(self):
        """
        Genera el archivo Excel del cruce y retorna la ruta completa del archivo generado.

        Returns:
            str: Ruta completa del archivo Excel generado
        """
        ruta_final = os.path.join(settings.MEDIA_ROOT, f'cruce_{self.fecha_inicio}_to_{self.fecha_fin}.xlsx')
        transacciones = self.transacciones.all()
        transacciones_convertidas = list(map(lambda t: t.convertir_en_diccionario(), transacciones))
        data_frame_transacciones = pd.DataFrame(transacciones_convertidas)
        if not data_frame_transacciones.empty and 'fecha' in data_frame_transacciones.columns:
            data_frame_transacciones['fecha'] = data_frame_transacciones['fecha'].dt.tz_localize(None)
        data_frame_transacciones.to_excel(ruta_final, index=False)
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
    reporte = models.ForeignKey(ReporteVtex, on_delete=models.CASCADE, related_name='transacciones')

    def convertir_en_diccionario(self):
        return {
            'Pedido': self.numero_pedido,
            'Transaccion': self.numero_transaccion,
            'fecha': self.fecha_hora,
            'medio_pago': self.medio_pago,
            'seller': self.seller,
            'estado': self.estado
        }