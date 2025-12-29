from django.urls import path

from . import views

urlpatterns = [
    # General
    path("", views.home, name="home"),
    path("ajustes", views.ajustes_view, name="ajustes"),

    # Payway
    path("reportesPayway", views.reportePaywayListView.as_view(), name="lista_reportes"),
    path("reportesPayway/generar", views.generar_reporte_payway_view, name="generar_reporte"),
    path("reportesPayway/<int:pk>/", views.reportePaywayDetailView.as_view(), name="detalle_reporte"),
    path("reportesPayway/<int:pk>/reporteExcel/", views.exportar_reporte_excel, name="exportar_reporte"),

    # VTEX
    path("reportesVtex", views.reporteVtexListView.as_view(), name="lista_reportes_vtex"),
    path("reportesVtex/generar", views.generar_reporte_vtex_view, name="generar_reporte_vtex"),
    path("reportesVtex/<int:pk>/", views.reporteVtexDetailView.as_view(), name="detalle_reporte_vtex"),
    path("reportesVtex/<int:pk>/reporteExcel/", views.exportar_reporte_vtex_excel, name="exportar_reporte_vtex_excel"),

    # CDP
    path("reportesCDP", views.reporteCDPListView.as_view(), name="lista_reportes_cdp"),
    path("reportesCDP/generar", views.generar_reporte_cdp_view, name="generar_reporte_cdp"),
    path("reportesCDP/<int:pk>/", views.reporteCDPDetailView.as_view(), name="detalle_reporte_cdp"),
    path("reportesCDP/<int:pk>/reporteExcel/", views.exportar_reporte_cdp_excel, name="exportar_reporte_cdp_excel"),

    # Janis
    path("reportesJanis", views.reporteJanisListView.as_view(), name="lista_reportes_janis"),
    path("reportesJanis/generar", views.generar_reporte_janis_view, name="generar_reporte_janis"),
    path("reportesJanis/importar", views.importar_reporte_janis_view, name="importar_reporte_janis"),
    path("reportesJanis/<int:pk>/", views.reporteJanisDetailView.as_view(), name="detalle_reporte_janis"),
    path("reportesJanis/<int:pk>/reporteExcel/", views.exportar_reporte_janis_excel, name="exportar_reporte_janis_excel"),

    # Cruces
    path("cruces", views.cruceListView.as_view(), name="lista_cruces"),
    path("cruces/generar", views.generar_cruce_view, name="generar_cruce"),
    path("cruces/<int:pk>/", views.cruceDetailView.as_view(), name="detalle_cruce"),
    path("cruces/<int:pk>/exportar/", views.exportar_cruce_excel, name="exportar_cruce_excel"),
]