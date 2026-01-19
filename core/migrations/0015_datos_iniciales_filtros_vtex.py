"""
Migración de datos: Carga inicial de tipos y valores de filtros VTEX.

También migra los datos existentes del campo JSONField a las nuevas tablas.
"""
from django.db import migrations


def cargar_datos_iniciales(apps, schema_editor):
    """Carga los tipos de filtros y sus valores iniciales."""
    TipoFiltroVtex = apps.get_model('core', 'TipoFiltroVtex')
    ValorFiltroVtex = apps.get_model('core', 'ValorFiltroVtex')

    # Crear tipo de filtro: Estado del pedido
    tipo_estado = TipoFiltroVtex.objects.create(
        codigo='estado',
        nombre='Estado del pedido',
        parametro_api='f_status',
        activo=True
    )

    tipo_estado_verificando_factura = TipoFiltroVtex.objects.create(
        codigo='estado_verificando_factura',
        nombre='Estado del pedido (verificando factura)',
        parametro_api='f_statusDescription',
        activo=True
    )
    estados_verificando_factura = [
        ('Verificando Fatura','Verficiando Factura')
    ]
    # Valores para el filtro de estado
    estados = [
        ('payment-pending', 'Pago Pendiente'),
        ('payment-approved', 'Pago Aprobado'),
        ('ready-for-handling', 'Listo para Preparar'),
        ('handling', 'En Preparación'),
        ('invoiced', 'Facturado'),
        ('canceled', 'Cancelado'),
    ]

    for codigo, nombre in estados:
        ValorFiltroVtex.objects.create(
            tipo_filtro=tipo_estado,
            codigo=codigo,
            nombre=nombre,
            activo=True
        )
    for codigo, nombre in estados_verificando_factura:
        ValorFiltroVtex.objects.create(
            tipo_filtro=tipo_estado_verificando_factura,
            codigo=codigo,
            nombre=nombre,
            activo=True
        )

def migrar_datos_existentes(apps, schema_editor):
    """
    Migra los filtros del campo JSONField a las nuevas tablas.
    """
    ReporteVtex = apps.get_model('core', 'ReporteVtex')
    TipoFiltroVtex = apps.get_model('core', 'TipoFiltroVtex')
    ValorFiltroVtex = apps.get_model('core', 'ValorFiltroVtex')
    FiltroReporteVtex = apps.get_model('core', 'FiltroReporteVtex')

    # Obtener el tipo de filtro de estado
    try:
        tipo_estado = TipoFiltroVtex.objects.get(codigo='estado')
    except TipoFiltroVtex.DoesNotExist:
        # Si no existe, no hay nada que migrar
        return

    # Migrar cada reporte que tenga filtros en el JSONField
    for reporte in ReporteVtex.objects.exclude(filtros__isnull=True):
        if not reporte.filtros:
            continue

        estados_filtrados = reporte.filtros.get('estados', [])

        for codigo_estado in estados_filtrados:
            try:
                valor_filtro = ValorFiltroVtex.objects.get(
                    tipo_filtro=tipo_estado,
                    codigo=codigo_estado
                )
                # Crear el registro en la tabla intermedia
                FiltroReporteVtex.objects.get_or_create(
                    reporte=reporte,
                    tipo_filtro=tipo_estado,
                    valor_filtro=valor_filtro
                )
            except ValorFiltroVtex.DoesNotExist:
                # Si el valor no existe, lo ignoramos
                pass


def revertir_datos(apps, schema_editor):
    """Revierte la migración eliminando los datos cargados."""
    TipoFiltroVtex = apps.get_model('core', 'TipoFiltroVtex')
    FiltroReporteVtex = apps.get_model('core', 'FiltroReporteVtex')

    # Eliminar todos los filtros de reportes
    FiltroReporteVtex.objects.all().delete()

    # Eliminar los tipos de filtro (cascade eliminará los valores)
    TipoFiltroVtex.objects.filter(codigo__in=['estado', 'estado_verificando_factura']).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0014_filtros_vtex_modelos'),
    ]

    operations = [
        migrations.RunPython(cargar_datos_iniciales, revertir_datos),
        migrations.RunPython(migrar_datos_existentes, migrations.RunPython.noop),
    ]
