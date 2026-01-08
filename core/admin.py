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