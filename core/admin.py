from django.contrib import admin

from core.models import (
    TransaccionPayway,
    ReportePayway,
    TransaccionVtex,
    ReporteVtex,
    UsuarioPayway,
    UsuarioCDP,
    UsuarioVtex,
    UsuarioJanis,
    TransaccionCDP,
    ReporteCDP,
    Cruce,
    TransaccionCruce,
    ReporteJanis,
    TransaccionJanis,
    TipoFiltroVtex,
    ValorFiltroVtex,
    FiltroReporteVtex,
    UsuarioCarrefourWeb,
    TareaCatalogacion,
    SellerVtex,
    ProductoVtex,
    SkuVtex,
    ConsultaVisibilidad,
)

# Register your models here.
# Payway
admin.site.register(TransaccionPayway)
admin.site.register(ReportePayway)

# VTEX
admin.site.register(TransaccionVtex)
admin.site.register(ReporteVtex)

admin.site.register(TransaccionCDP)
admin.site.register(ReporteCDP)

admin.site.register(Cruce)
admin.site.register(TransaccionCruce)

# Janis
admin.site.register(TransaccionJanis)
admin.site.register(ReporteJanis)

# Credenciales
admin.site.register(UsuarioPayway)
admin.site.register(UsuarioCDP)
admin.site.register(UsuarioVtex)
admin.site.register(UsuarioJanis)


# =============================================================================
# FILTROS VTEX - Administración de catálogos
# =============================================================================

class ValorFiltroVtexInline(admin.TabularInline):
    """Inline para editar valores de filtro dentro del tipo de filtro."""
    model = ValorFiltroVtex
    extra = 1
    fields = ['codigo', 'nombre', 'activo']


@admin.register(TipoFiltroVtex)
class TipoFiltroVtexAdmin(admin.ModelAdmin):
    """Admin para tipos de filtros VTEX."""
    list_display = ['nombre', 'codigo', 'parametro_api', 'activo']
    list_filter = ['activo']
    search_fields = ['nombre', 'codigo', 'parametro_api']
    ordering = ['nombre']
    inlines = [ValorFiltroVtexInline]


@admin.register(ValorFiltroVtex)
class ValorFiltroVtexAdmin(admin.ModelAdmin):
    """Admin para valores de filtros VTEX."""
    list_display = ['nombre', 'codigo', 'tipo_filtro', 'activo']
    list_filter = ['tipo_filtro', 'activo']
    search_fields = ['nombre', 'codigo']
    ordering = ['tipo_filtro', 'nombre']


@admin.register(FiltroReporteVtex)
class FiltroReporteVtexAdmin(admin.ModelAdmin):
    """Admin para filtros aplicados a reportes VTEX."""
    list_display = ['reporte', 'tipo_filtro', 'valor_filtro']
    list_filter = ['tipo_filtro', 'valor_filtro']
    search_fields = ['reporte__id']
    ordering = ['-reporte__id']
    raw_id_fields = ['reporte']


# =============================================================================
# CATALOGACION
# =============================================================================

@admin.register(UsuarioCarrefourWeb)
class UsuarioCarrefourWebAdmin(admin.ModelAdmin):
    list_display = ['email']


@admin.register(TareaCatalogacion)
class TareaCatalogacionAdmin(admin.ModelAdmin):
    list_display = ['id', 'tipo', 'estado', 'fecha_creacion', 'progreso_actual', 'progreso_total']
    list_filter = ['tipo', 'estado']
    ordering = ['-id']


@admin.register(SellerVtex)
class SellerVtexAdmin(admin.ModelAdmin):
    list_display = ['nombre', 'account_name', 'marketplace', 'url']
    search_fields = ['nombre', 'account_name']
    raw_id_fields = ['marketplace']


@admin.register(ProductoVtex)
class ProductoVtexAdmin(admin.ModelAdmin):
    list_display = ['productId']
    search_fields = ['productId']


@admin.register(SkuVtex)
class SkuVtexAdmin(admin.ModelAdmin):
    list_display = ['skuId', 'producto']
    search_fields = ['skuId']
    raw_id_fields = ['producto']


@admin.register(ConsultaVisibilidad)
class ConsultaVisibilidadAdmin(admin.ModelAdmin):
    list_display = ['sku', 'seller', 'visible', 'motivo', 'precio', 'stock', 'fecha']
    list_filter = ['visible', 'seller']
    search_fields = ['sku__skuId']
    raw_id_fields = ['sku', 'seller', 'tarea']
    ordering = ['-fecha']