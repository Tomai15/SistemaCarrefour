from django.urls import path

from . import views

urlpatterns = [
    # General
    path("", views.home, name="home"),
    path("ecommerce/", views.home_ecommerce, name="home_ecommerce"),
    path("catalogacion-marketplace/", views.home_catalogacion, name="home_catalogacion"),
    path("ajustes", views.ajustes_view, name="ajustes"),

    # Payway
    path("reportesPayway", views.reportePaywayListView.as_view(), name="lista_reportes"),
    path("reportesPayway/generar", views.generar_reporte_payway_view, name="generar_reporte"),
    path("reportesPayway/<int:pk>/", views.reportePaywayDetailView.as_view(), name="detalle_reporte"),
    path("reportesPayway/<int:pk>/reporteExcel/", views.exportar_reporte_excel, name="exportar_reporte"),
    path("reportesPayway/<int:pk>/reintentar/", views.ReportePaywayRetryView.as_view(), name="reintentar_reporte_payway"),
    path("reportesPayway/<int:pk>/eliminar/", views.ReportePaywayDeleteView.as_view(), name="eliminar_reporte_payway"),

    # VTEX
    path("reportesVtex", views.reporteVtexListView.as_view(), name="lista_reportes_vtex"),
    path("reportesVtex/generar", views.generar_reporte_vtex_view, name="generar_reporte_vtex"),
    path("reportesVtex/<int:pk>/", views.reporteVtexDetailView.as_view(), name="detalle_reporte_vtex"),
    path("reportesVtex/<int:pk>/reporteExcel/", views.exportar_reporte_vtex_excel, name="exportar_reporte_vtex_excel"),
    path("reportesVtex/<int:pk>/reintentar/", views.ReporteVtexRetryView.as_view(), name="reintentar_reporte_vtex"),
    path("reportesVtex/<int:pk>/eliminar/", views.ReporteVtexDeleteView.as_view(), name="eliminar_reporte_vtex"),

    # CDP
    path("reportesCDP", views.reporteCDPListView.as_view(), name="lista_reportes_cdp"),
    path("reportesCDP/generar", views.generar_reporte_cdp_view, name="generar_reporte_cdp"),
    path("reportesCDP/<int:pk>/", views.reporteCDPDetailView.as_view(), name="detalle_reporte_cdp"),
    path("reportesCDP/<int:pk>/reporteExcel/", views.exportar_reporte_cdp_excel, name="exportar_reporte_cdp_excel"),
    path("reportesCDP/<int:pk>/reintentar/", views.ReporteCDPRetryView.as_view(), name="reintentar_reporte_cdp"),
    path("reportesCDP/<int:pk>/eliminar/", views.ReporteCDPDeleteView.as_view(), name="eliminar_reporte_cdp"),

    # Janis
    path("reportesJanis", views.reporteJanisListView.as_view(), name="lista_reportes_janis"),
    path("reportesJanis/generar", views.generar_reporte_janis_view, name="generar_reporte_janis"),
    path("reportesJanis/importar", views.importar_reporte_janis_view, name="importar_reporte_janis"),
    path("reportesJanis/<int:pk>/", views.reporteJanisDetailView.as_view(), name="detalle_reporte_janis"),
    path("reportesJanis/<int:pk>/reporteExcel/", views.exportar_reporte_janis_excel, name="exportar_reporte_janis_excel"),
    path("reportesJanis/<int:pk>/reintentar/", views.ReporteJanisRetryView.as_view(), name="reintentar_reporte_janis"),
    path("reportesJanis/<int:pk>/eliminar/", views.ReporteJanisDeleteView.as_view(), name="eliminar_reporte_janis"),

    # Cruces
    path("cruces", views.cruceListView.as_view(), name="lista_cruces"),
    path("cruces/generar", views.generar_cruce_view, name="generar_cruce"),
    path("cruces/<int:pk>/", views.cruceDetailView.as_view(), name="detalle_cruce"),
    path("cruces/<int:pk>/exportar/", views.exportar_cruce_excel, name="exportar_cruce_excel"),
    path("cruces/<int:pk>/reintentar/", views.CruceRetryView.as_view(), name="reintentar_cruce"),
    path("cruces/<int:pk>/eliminar/", views.CruceDeleteView.as_view(), name="eliminar_cruce"),

    # Catalogacion
    path("catalogacion/", views.TareaCatalogacionListView.as_view(), name="lista_tareas_catalogacion"),
    path("catalogacion/<int:pk>/", views.TareaCatalogacionDetailView.as_view(), name="detalle_tarea_catalogacion"),
    path("catalogacion/<int:pk>/descargar/", views.descargar_resultado_tarea, name="descargar_resultado_tarea"),
    path("catalogacion/<int:pk>/eliminar/", views.TareaCatalogacionDeleteView.as_view(), name="eliminar_tarea_catalogacion"),
    path("catalogacion/busqueda-eans/", views.busqueda_eans_view, name="busqueda_eans"),
    path("catalogacion/busqueda-categorias/", views.busqueda_categorias_view, name="busqueda_categorias"),
    path("catalogacion/sellers-externos/", views.sellers_externos_view, name="sellers_externos"),
    path("catalogacion/sellers-no-carrefour/", views.sellers_no_carrefour_view, name="sellers_no_carrefour"),
    path("catalogacion/actualizar-modal/", views.actualizar_modal_view, name="actualizar_modal"),
    path("catalogacion/consulta-visibilidad/", views.consulta_visibilidad_view, name="consulta_visibilidad"),

    # Plantillas de ejemplo
    path("plantilla/<str:tipo>/", views.descargar_plantilla, name="descargar_plantilla"),
]