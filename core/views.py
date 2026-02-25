from __future__ import annotations

from typing import Any

from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, DetailView, View, DeleteView
from django.views.generic.detail import SingleObjectMixin
from django.http import FileResponse, Http404, HttpRequest, HttpResponse
import csv
import io
from django.urls import reverse_lazy
from django.contrib import messages
from django.db.models import Sum, Count, Q, QuerySet
import os

from django_q.tasks import async_task

from core.models import (
    ReportePayway, ReporteVtex, ReporteCDP, ReporteJanis, Cruce,
    UsuarioPayway, UsuarioCDP, UsuarioCarrefourWeb,
    ValorFiltroVtex, FiltroReporteVtex,
    TareaCatalogacion, SellerVtex
)
from core.forms import (
    GenerarReportePaywayForm,
    GenerarReporteVtexForm,
    GenerarReporteCDPForm,
    GenerarReporteJanisForm,
    GenerarCruceForm,
    CredencialesPaywayForm,
    CredencialesCDPForm,
    CredencialesCarrefourWebForm,
    BusquedaEansForm,
    BusquedaCategoriasForm,
    SellersExternosForm,
    SellersNoCarrefourForm,
    ActualizarModalForm,
    ConsultaVisibilidadForm
)
from datetime import date
import csv
import pandas as pd


# Create your views here.
def home(request: HttpRequest) -> HttpResponse:
    return render(request, 'core/home.html')


def home_ecommerce(request: HttpRequest) -> HttpResponse:
    return render(request, 'core/homeEcommerce.html')


def home_catalogacion(request: HttpRequest) -> HttpResponse:
    return render(request, 'core/homeCatalogacion.html')

class reportePaywayListView(ListView):
    model = ReportePayway
    paginate_by = 50
    template_name = 'core/Payway/vistaReportes.html'
    ordering = ['-id']


class reportePaywayDetailView(SingleObjectMixin, ListView):
    """
    Vista de detalle de reporte con paginación server-side de transacciones.

    Combina SingleObjectMixin (para obtener el reporte) con ListView (para paginar transacciones).
    Esto permite manejar eficientemente reportes con miles de transacciones.
    """
    template_name = 'core/Payway/detalleReporte.html'
    paginate_by = 20  # Transacciones por página
    context_object_name = 'transacciones'

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReportePayway.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self) -> QuerySet[Any]:
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context: dict[str, Any] = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_excel(request: HttpRequest, pk: int) -> HttpResponse:
    """Vista para exportar un reporte de Payway a Excel."""
    reporte = get_object_or_404(ReportePayway, pk=pk)

    # El modelo es responsable de generar el archivo y retornar su ruta
    ruta_archivo = reporte.generar_reporter_excel()

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_reporte_payway_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para generar un nuevo reporte de Payway.

    Muestra un formulario para ingresar fechas y encola la generación del reporte.
    """
    if request.method == 'POST':
        form = GenerarReportePaywayForm(request.POST)

        if form.is_valid():
            # Verificar que existan credenciales configuradas
            credenciales = UsuarioPayway.objects.first()

            if not credenciales:
                messages.error(
                    request,
                    'No hay credenciales de Payway configuradas. '
                    'Por favor, configure las credenciales en Ajustes antes de generar un reporte.'
                )
                return render(request, 'core/Payway/generarReporte.html', {'form': form})

            # Obtener fechas del formulario
            fecha_inicio = form.cleaned_data['fecha_inicio']
            fecha_fin = form.cleaned_data['fecha_fin']

            # Formatear fechas para el servicio (DD/MM/YYYY)
            fecha_inicio_str = fecha_inicio.strftime("%d/%m/%Y")
            fecha_fin_str = fecha_fin.strftime("%d/%m/%Y")

            # Crear el reporte en estado PENDIENTE
            nuevo_reporte = ReportePayway.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReportePayway.Estado.PENDIENTE
            )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_reporte_payway_async',
                    fecha_inicio_str,
                    fecha_fin_str,
                    nuevo_reporte.id  # Pasar solo el ID, no el objeto completo
                )

                messages.success(
                    request,
                    f'Reporte encolado exitosamente. Puede visualizarlo en Reportes Generados.'
                )

                return redirect('lista_reportes')

            except Exception as e:
                messages.error(
                    request,
                    f'Error al crear el reporte: {str(e)}'
                )

    else:
        form = GenerarReportePaywayForm()

    return render(request, 'core/Payway/generarReporte.html', {'form': form})


# ==================== VISTAS VTEX ====================

class reporteVtexListView(ListView):
    """Vista de lista de reportes de VTEX con paginación."""
    model = ReporteVtex
    paginate_by = 50
    template_name = 'core/Vtex/vistaReportes.html'
    ordering = ['-id']


class reporteVtexDetailView(SingleObjectMixin, ListView):
    """
    Vista de detalle de reporte VTEX con paginación server-side de transacciones.

    Combina SingleObjectMixin (para obtener el reporte) con ListView (para paginar transacciones).
    Esto permite manejar eficientemente reportes con miles de transacciones.
    """
    template_name = 'core/Vtex/detalleReporte.html'
    paginate_by = 20  # Transacciones por página
    context_object_name = 'transacciones'

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteVtex.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self) -> QuerySet[Any]:
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context: dict[str, Any] = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_vtex_excel(request: HttpRequest, pk: int) -> HttpResponse:
    """Vista para exportar un reporte de VTEX a Excel."""
    reporte = get_object_or_404(ReporteVtex, pk=pk)

    # El modelo es responsable de generar el archivo y retornar su ruta
    ruta_archivo = reporte.generar_reporter_excel()

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_reporte_vtex_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para generar un nuevo reporte de VTEX.

    Muestra un formulario para ingresar fechas y filtros, y encola la generación del reporte.
    """
    if request.method == 'POST':
        form = GenerarReporteVtexForm(request.POST)

        if form.is_valid():
            # Obtener fechas del formulario
            fecha_inicio = form.cleaned_data['fecha_inicio']
            fecha_fin = form.cleaned_data['fecha_fin']
            filtros_estado = form.cleaned_data.get('filtros_estado', [])
            incluir_sellers = form.cleaned_data.get('incluir_sellers', True)

            # Formatear fechas para el servicio (DD/MM/YYYY)
            fecha_inicio_str = fecha_inicio.strftime("%d/%m/%Y")
            fecha_fin_str = fecha_fin.strftime("%d/%m/%Y")

            # Crear el reporte en estado PENDIENTE
            nuevo_reporte = ReporteVtex.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReporteVtex.Estado.PENDIENTE,
                incluir_sellers=incluir_sellers
            )

            # Crear los registros de filtros aplicados
            # Cada valor_filtro tiene su propio tipo_filtro (pueden tener diferentes parametros de API)
            for valor_filtro in filtros_estado:
                FiltroReporteVtex.objects.create(
                    reporte=nuevo_reporte,
                    tipo_filtro=valor_filtro.tipo_filtro,
                    valor_filtro=valor_filtro
                )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_reporte_vtex_async',
                    fecha_inicio_str,
                    fecha_fin_str,
                    nuevo_reporte.id  # El servicio obtendrá los filtros del reporte
                )

                messages.success(
                    request,
                    f'Reporte de VTEX encolado exitosamente. Puede visualizarlo en Reportes Generados.'
                )

                return redirect('lista_reportes_vtex')

            except Exception as e:
                messages.error(
                    request,
                    f'Error al crear el reporte de VTEX: {str(e)}'
                )

    else:
        form = GenerarReporteVtexForm()

    return render(request, 'core/Vtex/generarReporte.html', {'form': form})


# ==================== VISTA DE AJUSTES ====================

def ajustes_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para gestionar credenciales de Payway, CDP y Carrefour Web.

    Permite ver y editar las credenciales de todas las plataformas.
    """
    credenciales_payway = UsuarioPayway.objects.first()
    credenciales_cdp = UsuarioCDP.objects.first()
    credenciales_carrefour = UsuarioCarrefourWeb.objects.first()

    if request.method == 'POST':
        if 'payway_submit' in request.POST:
            form_payway = CredencialesPaywayForm(request.POST, instance=credenciales_payway)

            if form_payway.is_valid():
                form_payway.save()
                messages.success(request, 'Credenciales de Payway actualizadas exitosamente.')
                return redirect('ajustes')

            form_cdp = CredencialesCDPForm(instance=credenciales_cdp)
            form_carrefour = CredencialesCarrefourWebForm(instance=credenciales_carrefour)

        elif 'cdp_submit' in request.POST:
            form_cdp = CredencialesCDPForm(request.POST, instance=credenciales_cdp)

            if form_cdp.is_valid():
                form_cdp.save()
                messages.success(request, 'Credenciales de CDP actualizadas exitosamente.')
                return redirect('ajustes')

            form_payway = CredencialesPaywayForm(instance=credenciales_payway)
            form_carrefour = CredencialesCarrefourWebForm(instance=credenciales_carrefour)

        elif 'carrefour_submit' in request.POST:
            form_carrefour = CredencialesCarrefourWebForm(request.POST, instance=credenciales_carrefour)

            if form_carrefour.is_valid():
                form_carrefour.save()
                messages.success(request, 'Credenciales de Carrefour Web actualizadas exitosamente.')
                return redirect('ajustes')

            form_payway = CredencialesPaywayForm(instance=credenciales_payway)
            form_cdp = CredencialesCDPForm(instance=credenciales_cdp)

        else:
            form_payway = CredencialesPaywayForm(instance=credenciales_payway)
            form_cdp = CredencialesCDPForm(instance=credenciales_cdp)
            form_carrefour = CredencialesCarrefourWebForm(instance=credenciales_carrefour)

    else:
        form_payway = CredencialesPaywayForm(instance=credenciales_payway)
        form_cdp = CredencialesCDPForm(instance=credenciales_cdp)
        form_carrefour = CredencialesCarrefourWebForm(instance=credenciales_carrefour)

    context = {
        'form_payway': form_payway,
        'form_cdp': form_cdp,
        'form_carrefour': form_carrefour,
        'tiene_payway': credenciales_payway is not None,
        'tiene_cdp': credenciales_cdp is not None,
        'tiene_carrefour': credenciales_carrefour is not None,
    }

    return render(request, 'core/ajustes.html', context)


# ==================== VISTAS CDP ====================

class reporteCDPListView(ListView):
    """Vista de lista de reportes de CDP con paginación."""
    model = ReporteCDP
    paginate_by = 50
    template_name = 'core/CDP/vistaReportes.html'
    ordering = ['-id']


class reporteCDPDetailView(SingleObjectMixin, ListView):
    """
    Vista de detalle de reporte CDP con paginación server-side de transacciones.

    Combina SingleObjectMixin (para obtener el reporte) con ListView (para paginar transacciones).
    Esto permite manejar eficientemente reportes con miles de transacciones.
    """
    template_name = 'core/CDP/detalleReporte.html'
    paginate_by = 20  # Transacciones por página
    context_object_name = 'transacciones'

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteCDP.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self) -> QuerySet[Any]:
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context: dict[str, Any] = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_cdp_excel(request: HttpRequest, pk: int) -> HttpResponse:
    """Vista para exportar un reporte de CDP a Excel."""
    reporte = get_object_or_404(ReporteCDP, pk=pk)

    # El modelo es responsable de generar el archivo y retornar su ruta
    ruta_archivo = reporte.generar_reporter_excel()

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_reporte_cdp_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para generar un nuevo reporte de CDP.

    Muestra un formulario para ingresar fechas y encola la generación del reporte.
    """
    if request.method == 'POST':
        form = GenerarReporteCDPForm(request.POST)

        if form.is_valid():
            # Verificar que existan credenciales configuradas
            credenciales = UsuarioCDP.objects.first()

            if not credenciales:
                messages.error(
                    request,
                    'No hay credenciales de CDP configuradas. '
                    'Por favor, configure las credenciales en Ajustes antes de generar un reporte.'
                )
                return render(request, 'core/CDP/generarReporte.html', {'form': form})

            # Obtener fechas del formulario
            fecha_inicio = form.cleaned_data['fecha_inicio']
            fecha_fin = form.cleaned_data['fecha_fin']

            # Formatear fechas para el servicio (DD/MM/YYYY)
            fecha_inicio_str = fecha_inicio.strftime("%d/%m/%Y")
            fecha_fin_str = fecha_fin.strftime("%d/%m/%Y")

            # Crear el reporte en estado PENDIENTE
            nuevo_reporte = ReporteCDP.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReporteCDP.Estado.PENDIENTE
            )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_reporte_cdp_async',
                    fecha_inicio_str,
                    fecha_fin_str,
                    nuevo_reporte.id  # Pasar solo el ID, no el objeto completo
                )

                messages.success(
                    request,
                    f'Reporte de CDP encolado exitosamente. Puede visualizarlo en Reportes Generados.'
                )

                return redirect('lista_reportes_cdp')

            except Exception as e:
                messages.error(
                    request,
                    f'Error al crear el reporte de CDP: {str(e)}'
                )

    else:
        form = GenerarReporteCDPForm()

    return render(request, 'core/CDP/generarReporte.html', {'form': form})


# ==================== VISTAS JANIS ====================

class reporteJanisListView(ListView):
    """Vista de lista de reportes de Janis con paginación."""
    model = ReporteJanis
    paginate_by = 50
    template_name = 'core/Janis/vistaReportes.html'
    ordering = ['-id']


class reporteJanisDetailView(SingleObjectMixin, ListView):
    """
    Vista de detalle de reporte Janis con paginación server-side de transacciones.

    Combina SingleObjectMixin (para obtener el reporte) con ListView (para paginar transacciones).
    Esto permite manejar eficientemente reportes con miles de transacciones.
    """
    template_name = 'core/Janis/detalleReporte.html'
    paginate_by = 20  # Transacciones por página
    context_object_name = 'transacciones'

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteJanis.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self) -> QuerySet[Any]:
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context: dict[str, Any] = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_janis_excel(request: HttpRequest, pk: int) -> HttpResponse:
    """Vista para exportar un reporte de Janis a Excel."""
    reporte = get_object_or_404(ReporteJanis, pk=pk)

    # El modelo es responsable de generar el archivo y retornar su ruta
    ruta_archivo = reporte.generar_reporter_excel()

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_reporte_janis_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para generar un nuevo reporte de Janis.

    Muestra un formulario para ingresar fechas y encola la generación del reporte.
    """
    if request.method == 'POST':
        form = GenerarReporteJanisForm(request.POST)

        if form.is_valid():
            # Obtener fechas del formulario
            fecha_inicio = form.cleaned_data['fecha_inicio']
            fecha_fin = form.cleaned_data['fecha_fin']

            # Formatear fechas para el servicio (DD/MM/YYYY)
            fecha_inicio_str = fecha_inicio.strftime("%d/%m/%Y")
            fecha_fin_str = fecha_fin.strftime("%d/%m/%Y")

            # Crear el reporte en estado PENDIENTE
            nuevo_reporte = ReporteJanis.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReporteJanis.Estado.PENDIENTE
            )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_reporte_janis_async',
                    fecha_inicio_str,
                    fecha_fin_str,
                    nuevo_reporte.id  # Pasar solo el ID, no el objeto completo
                )

                messages.success(
                    request,
                    f'Reporte de Janis encolado exitosamente. Puede visualizarlo en Reportes Generados.'
                )

                return redirect('lista_reportes_janis')

            except Exception as e:
                messages.error(
                    request,
                    f'Error al crear el reporte de Janis: {str(e)}'
                )

    else:
        form = GenerarReporteJanisForm()

    return render(request, 'core/Janis/generarReporte.html', {'form': form})


def importar_reporte_janis_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para importar un reporte de Janis desde un archivo Excel.

    Procesa el archivo subido y crea las transacciones en la base de datos.
    """
    if request.method == 'POST':
        archivo = request.FILES.get('archivo_excel')
        fecha_inicio_str = request.POST.get('fecha_inicio')
        fecha_fin_str = request.POST.get('fecha_fin')

        # Validaciones básicas
        if not archivo:
            messages.error(request, 'Debe seleccionar un archivo Excel.')
            return redirect('generar_reporte_janis')

        if not fecha_inicio_str or not fecha_fin_str:
            messages.error(request, 'Debe ingresar las fechas de inicio y fin.')
            return redirect('generar_reporte_janis')

        # Validar extensión del archivo
        if not archivo.name.endswith(('.xlsx', '.xls')):
            messages.error(request, 'El archivo debe ser un Excel (.xlsx o .xls).')
            return redirect('generar_reporte_janis')

        try:
            # Parsear fechas
            from datetime import datetime
            fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()
            fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d').date()

            # Validar que fecha_inicio <= fecha_fin
            if fecha_inicio > fecha_fin:
                messages.error(request, 'La fecha de inicio no puede ser posterior a la fecha de fin.')
                return redirect('generar_reporte_janis')

            # Crear el reporte
            nuevo_reporte = ReporteJanis.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReporteJanis.Estado.PROCESANDO
            )

            # Importar usando el servicio
            from core.services.ReporteJanisService import ReporteJanisService
            servicio = ReporteJanisService()
            cantidad = servicio.importar_desde_excel(archivo, nuevo_reporte)

            # Actualizar estado a COMPLETADO
            nuevo_reporte.estado = ReporteJanis.Estado.COMPLETADO
            nuevo_reporte.save()

            messages.success(
                request,
                f'Reporte importado exitosamente. {cantidad} transacciones procesadas.'
            )
            return redirect('lista_reportes_janis')

        except Exception as e:
            messages.error(request, f'Error al importar el archivo: {str(e)}')
            # Si se creó el reporte, marcarlo como error
            if 'nuevo_reporte' in locals():
                nuevo_reporte.estado = ReporteJanis.Estado.ERROR
                nuevo_reporte.save()
            return redirect('generar_reporte_janis')

    # Si es GET, redirigir al formulario
    return redirect('generar_reporte_janis')


# ==================== VISTAS CRUCES ====================

class cruceListView(ListView):
    """Vista de lista de cruces con paginación."""
    model = Cruce
    paginate_by = 50
    template_name = 'core/Cruce/vistaCruces.html'
    ordering = ['-id']


class cruceDetailView(SingleObjectMixin, ListView):
    """
    Vista de detalle de cruce con paginación server-side de transacciones.

    Combina SingleObjectMixin (para obtener el cruce) con ListView (para paginar transacciones).
    """
    template_name = 'core/Cruce/detalleCruce.html'
    paginate_by = 20
    context_object_name = 'transacciones'

    def get(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object = self.get_object(queryset=Cruce.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self) -> QuerySet[Any]:
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context: dict[str, Any] = super().get_context_data(**kwargs)
        context['cruce'] = self.object
        return context


def exportar_cruce_excel(request: HttpRequest, pk: int) -> HttpResponse:
    """Vista para exportar un cruce a Excel.

    Soporta filtros de columnas:
    - incluir_observaciones=1 (default): incluye columna resultado_cruce
    - incluir_precio_payway=1: incluye columnas monto_payway y monto_payway_2
    - incluir_precio_vtex=1: incluye columna valor_vtex
    """
    cruce = get_object_or_404(Cruce, pk=pk)

    incluir_observaciones = request.GET.get('incluir_observaciones') == '1'
    incluir_precio_payway = request.GET.get('incluir_precio_payway') == '1'
    incluir_precio_vtex = request.GET.get('incluir_precio_vtex') == '1'

    ruta_archivo = cruce.generar_reporter_excel(
        incluir_observaciones=incluir_observaciones,
        incluir_precio_payway=incluir_precio_payway,
        incluir_precio_vtex=incluir_precio_vtex
    )

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_cruce_view(request: HttpRequest) -> HttpResponse:
    """
    Vista para generar un nuevo cruce de reportes.

    Muestra un formulario para seleccionar reportes y encola la generación del cruce.
    """
    if request.method == 'POST':
        form = GenerarCruceForm(request.POST)

        if form.is_valid():
            reporte_vtex = form.cleaned_data.get('reporte_vtex')
            reporte_payway = form.cleaned_data.get('reporte_payway')
            reporte_cdp = form.cleaned_data.get('reporte_cdp')
            reporte_janis = form.cleaned_data.get('reporte_janis')

            # Determinar fecha_inicio y fecha_fin basándose en los reportes seleccionados
            fechas_inicio = []
            fechas_fin = []

            if reporte_vtex:
                fechas_inicio.append(reporte_vtex.fecha_inicio)
                fechas_fin.append(reporte_vtex.fecha_fin)
            if reporte_payway:
                fechas_inicio.append(reporte_payway.fecha_inicio)
                fechas_fin.append(reporte_payway.fecha_fin)
            if reporte_cdp:
                fechas_inicio.append(reporte_cdp.fecha_inicio)
                fechas_fin.append(reporte_cdp.fecha_fin)
            if reporte_janis:
                fechas_inicio.append(reporte_janis.fecha_inicio)
                fechas_fin.append(reporte_janis.fecha_fin)

            # Usar la fecha más temprana como inicio y la más tardía como fin
            fecha_inicio = min(fechas_inicio)
            fecha_fin = max(fechas_fin)

            # Crear el cruce en estado PENDIENTE con referencias a los reportes
            nuevo_cruce = Cruce.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=Cruce.Estado.PENDIENTE,
                reporte_vtex=reporte_vtex,
                reporte_payway=reporte_payway,
                reporte_cdp=reporte_cdp,
                reporte_janis=reporte_janis
            )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_cruce_async',
                    nuevo_cruce.id,
                    reporte_vtex.id if reporte_vtex else None,
                    reporte_payway.id if reporte_payway else None,
                    reporte_cdp.id if reporte_cdp else None,
                    reporte_janis.id if reporte_janis else None
                )

                messages.success(
                    request,
                    f'Cruce encolado exitosamente. Puede visualizarlo en la lista de cruces.'
                )

                return redirect('lista_cruces')

            except Exception as e:
                messages.error(
                    request,
                    f'Error al crear el cruce: {str(e)}'
                )

    else:
        form = GenerarCruceForm()

    return render(request, 'core/Cruce/generarCruce.html', {'form': form})


# ============================================================================
# MIXINS Y CLASS-BASED VIEWS PARA REINTENTAR Y ELIMINAR
# ============================================================================

class ReporteRetryMixin:
    """
    Mixin que provee lógica común para reintentar reportes fallidos.

    Las clases que hereden deben definir:
        - model: El modelo del reporte (ReportePayway, ReporteVtex, etc.)
        - task_name: Nombre de la tarea async ('core.tasks.generar_reporte_payway_async')
        - success_url: URL a la que redirigir después del reintento
    """
    model: Any = None
    task_name: str | None = None
    success_url: str | None = None

    def post(self, request: HttpRequest, pk: int) -> HttpResponse:
        reporte = get_object_or_404(self.model, pk=pk)

        if reporte.estado != self.model.Estado.ERROR:
            messages.warning(request, 'Solo se pueden reintentar reportes con estado ERROR.')
            return redirect(self.success_url)

        # Resetear estado
        reporte.estado = self.model.Estado.PENDIENTE
        reporte.save()

        # Formatear fechas
        fecha_inicio_str = reporte.fecha_inicio.strftime('%d/%m/%Y')
        fecha_fin_str = reporte.fecha_fin.strftime('%d/%m/%Y')

        # Encolar tarea
        try:
            async_task(self.task_name, fecha_inicio_str, fecha_fin_str, reporte.id)
            messages.success(request, f'Reporte #{reporte.id} encolado para reintento.')
        except Exception as e:
            messages.error(request, f'Error al encolar reintento: {str(e)}')

        return redirect(self.success_url)


class ReporteDeleteMixin:
    """
    Mixin que provee lógica común para eliminar reportes.

    Usa DeleteView de Django pero agrega mensaje de éxito.
    Las clases que hereden deben definir:
        - model: El modelo del reporte
        - success_url: URL a la que redirigir
    """
    template_name: str = 'core/confirm_delete.html'  # Template genérico (opcional)

    def form_valid(self, form: Any) -> HttpResponse:
        reporte_id = self.object.id
        response = super().form_valid(form)  # type: ignore[misc]
        messages.success(self.request, f'Reporte #{reporte_id} eliminado correctamente.')
        return response


# --- PAYWAY ---
class ReportePaywayRetryView(ReporteRetryMixin, View):
    model = ReportePayway
    task_name = 'core.tasks.generar_reporte_payway_async'
    success_url = 'lista_reportes'


class ReportePaywayDeleteView(ReporteDeleteMixin, DeleteView):
    model = ReportePayway
    success_url = reverse_lazy('lista_reportes')


# --- VTEX ---
class ReporteVtexRetryView(View):
    """
    Vista para reintentar reportes VTEX fallidos.

    Los filtros ya están guardados en el reporte mediante FiltroReporteVtex,
    el servicio los obtiene automáticamente.
    """
    def post(self, request: HttpRequest, pk: int) -> HttpResponse:
        reporte = get_object_or_404(ReporteVtex, pk=pk)

        if reporte.estado != ReporteVtex.Estado.ERROR:
            messages.warning(request, 'Solo se pueden reintentar reportes con estado ERROR.')
            return redirect('lista_reportes_vtex')

        # Resetear estado
        reporte.estado = ReporteVtex.Estado.PENDIENTE
        reporte.save()

        # Formatear fechas
        fecha_inicio_str = reporte.fecha_inicio.strftime('%d/%m/%Y')
        fecha_fin_str = reporte.fecha_fin.strftime('%d/%m/%Y')

        # Encolar tarea (los filtros se obtienen del reporte en el servicio)
        try:
            async_task(
                'core.tasks.generar_reporte_vtex_async',
                fecha_inicio_str,
                fecha_fin_str,
                reporte.id
            )
            messages.success(request, f'Reporte VTEX #{reporte.id} encolado para reintento.')
        except Exception as e:
            messages.error(request, f'Error al encolar reintento: {str(e)}')

        return redirect('lista_reportes_vtex')


class ReporteVtexDeleteView(ReporteDeleteMixin, DeleteView):
    model = ReporteVtex
    success_url = reverse_lazy('lista_reportes_vtex')


# --- CDP ---
class ReporteCDPRetryView(ReporteRetryMixin, View):
    model = ReporteCDP
    task_name = 'core.tasks.generar_reporte_cdp_async'
    success_url = 'lista_reportes_cdp'


class ReporteCDPDeleteView(ReporteDeleteMixin, DeleteView):
    model = ReporteCDP
    success_url = reverse_lazy('lista_reportes_cdp')


# --- JANIS ---
class ReporteJanisRetryView(ReporteRetryMixin, View):
    model = ReporteJanis
    task_name = 'core.tasks.generar_reporte_janis_async'
    success_url = 'lista_reportes_janis'


class ReporteJanisDeleteView(ReporteDeleteMixin, DeleteView):
    model = ReporteJanis
    success_url = reverse_lazy('lista_reportes_janis')


# --- CRUCES ---
class CruceRetryView(View):
    """
    Vista para reintentar cruces fallidos.

    Es diferente a los reportes porque usa los ForeignKeys
    para obtener los IDs de los reportes relacionados.
    """
    def post(self, request: HttpRequest, pk: int) -> HttpResponse:
        cruce = get_object_or_404(Cruce, pk=pk)

        if cruce.estado != Cruce.Estado.ERROR:
            messages.warning(request, 'Solo se pueden reintentar cruces con estado ERROR.')
            return redirect('lista_cruces')

        cruce.estado = Cruce.Estado.PENDIENTE
        cruce.save()

        try:
            async_task(
                'core.tasks.generar_cruce_async',
                cruce.id,
                cruce.reporte_vtex.id if cruce.reporte_vtex else None,
                cruce.reporte_payway.id if cruce.reporte_payway else None,
                cruce.reporte_cdp.id if cruce.reporte_cdp else None,
                cruce.reporte_janis.id if cruce.reporte_janis else None
            )
            messages.success(request, f'Cruce #{cruce.id} encolado para reintento.')
        except Exception as e:
            messages.error(request, f'Error al encolar reintento: {str(e)}')

        return redirect('lista_cruces')


class CruceDeleteView(ReporteDeleteMixin, DeleteView):
    model = Cruce
    success_url = reverse_lazy('lista_cruces')

    def form_valid(self, form: Any) -> HttpResponse:
        cruce_id = self.object.id
        response = super(DeleteView, self).form_valid(form)
        messages.success(self.request, f'Cruce #{cruce_id} eliminado correctamente.')
        return response


# =============================================================================
# CATALOGACION - VIEWS
# =============================================================================

class TareaCatalogacionListView(ListView):
    model = TareaCatalogacion
    paginate_by = 50
    template_name = 'core/Catalogacion/listaTareas.html'
    ordering = ['-id']


class TareaCatalogacionDetailView(DetailView):
    model = TareaCatalogacion
    template_name = 'core/Catalogacion/detalleTarea.html'
    context_object_name = 'tarea'

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        tarea = self.object
        if tarea.tipo == TareaCatalogacion.TipoTarea.CONSULTA_VISIBILIDAD:
            context['resultados_visibilidad'] = tarea.consultas_visibilidad.select_related('sku', 'seller').all()
        return context


class TareaCatalogacionDeleteView(DeleteView):
    model = TareaCatalogacion
    template_name = 'core/confirm_delete.html'
    success_url = reverse_lazy('lista_tareas_catalogacion')

    def form_valid(self, form):
        messages.success(self.request, f"Tarea #{self.object.id} eliminada correctamente.")
        return super().form_valid(form)


def descargar_resultado_tarea(request: HttpRequest, pk: int) -> HttpResponse:
    tarea = get_object_or_404(TareaCatalogacion, pk=pk)
    if not tarea.archivo_resultado:
        raise Http404("No hay archivo de resultado")
    ruta = tarea.archivo_resultado.path
    if not os.path.exists(ruta):
        raise Http404("El archivo no existe")
    return FileResponse(
        open(ruta, 'rb'),
        as_attachment=True,
        filename=os.path.basename(ruta)
    )


def actualizar_modal_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/actualizarModal.html'
    if request.method == 'POST':
        form = ActualizarModalForm(request.POST, request.FILES)
        if form.is_valid():
            archivo = request.FILES['archivo_excel']
            try:
                df = pd.read_excel(archivo)
                columnas = [c.strip().lower() for c in df.columns]
                df.columns = columnas

                if 'skuid' not in columnas or 'modal logistica' not in columnas:
                    messages.error(request, "El Excel debe tener las columnas 'skuid' y 'modal logistica'")
                    return render(request, template, {'form': form})

                valores_validos = {'FIREARMS', 'GLASS'}
                lista_skus = []
                for _, row in df.iterrows():
                    skuid = str(row['skuid']).strip()
                    modal = str(row['modal logistica']).strip().upper()
                    if not skuid or skuid == 'nan':
                        continue
                    if modal not in valores_validos:
                        messages.error(request, f"Valor invalido '{modal}' para SKU {skuid}. Solo FIREARMS o GLASS.")
                        return render(request, template, {'form': form})
                    lista_skus.append({'skuid': skuid, 'modal': modal})

                if not lista_skus:
                    messages.error(request, "El archivo no contiene datos validos.")
                    return render(request, template, {'form': form})

                tarea = TareaCatalogacion.objects.create(
                    tipo=TareaCatalogacion.TipoTarea.ACTUALIZAR_MODAL,
                    progreso_total=len(lista_skus)
                )
                async_task('core.tasks.actualizar_modal_async', tarea.id, lista_skus)
                messages.success(request, f"Tarea #{tarea.id} creada. Procesando {len(lista_skus)} SKUs.")
                return redirect('detalle_tarea_catalogacion', pk=tarea.id)

            except Exception as e:
                messages.error(request, f"Error leyendo Excel: {e}")
                return render(request, template, {'form': form})
        else:
            return render(request, template, {'form': form})
    else:
        form = ActualizarModalForm()
    return render(request, template, {'form': form})


def busqueda_eans_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/busquedaEans.html'
    if request.method == 'POST':
        form = BusquedaEansForm(request.POST, request.FILES)
        if form.is_valid():
            archivo = request.FILES['archivo_csv']
            try:
                stream = archivo.read().decode('utf-8').splitlines()
                reader = csv.reader(stream)
                next(reader, None)
                eans = [row[0].strip() for row in reader if row and row[0].strip()]
            except Exception as e:
                messages.error(request, f"Error leyendo CSV: {e}")
                return render(request, template, {'form': form})

            if not eans:
                messages.error(request, "El archivo no contiene EANs.")
                return render(request, template, {'form': form})

            direccion = form.cleaned_data['direccion']
            tipo_regio = form.cleaned_data['tipo_regio']
            n_workers = int(form.cleaned_data['cantidad_workers'])
            headless = not form.cleaned_data.get('ver_navegador', False)

            tarea = TareaCatalogacion.objects.create(
                tipo=TareaCatalogacion.TipoTarea.BUSQUEDA_EANS,
                progreso_total=len(eans)
            )
            async_task('core.tasks.busqueda_eans_async', tarea.id, eans, direccion, tipo_regio, n_workers, headless)
            messages.success(request, f"Tarea #{tarea.id} creada. Buscando {len(eans)} EANs.")
            return redirect('detalle_tarea_catalogacion', pk=tarea.id)
        else:
            return render(request, template, {'form': form})
    else:
        form = BusquedaEansForm()
    return render(request, template, {'form': form})


def busqueda_categorias_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/busquedaCategorias.html'
    if request.method == 'POST':
        form = BusquedaCategoriasForm(request.POST, request.FILES)
        if form.is_valid():
            archivo = request.FILES['archivo_csv']
            try:
                stream = archivo.read().decode('utf-8').splitlines()
                reader = csv.reader(stream)
                next(reader, None)
                categorias = [row[0].strip() for row in reader if row and row[0].strip()]
            except Exception as e:
                messages.error(request, f"Error leyendo CSV: {e}")
                return render(request, template, {'form': form})

            direcciones = request.POST.getlist('direcciones[]')
            if not direcciones or not any(d.strip() for d in direcciones):
                messages.error(request, "Agrega al menos una direccion.")
                return render(request, template, {'form': form})

            direcciones = [d.strip() for d in direcciones if d.strip()]
            tipo_regio = form.cleaned_data['tipo_regio']
            headless = not form.cleaned_data.get('ver_navegador', False)

            tarea = TareaCatalogacion.objects.create(
                tipo=TareaCatalogacion.TipoTarea.BUSQUEDA_CATEGORIAS,
                progreso_total=len(direcciones) * len(categorias)
            )
            async_task('core.tasks.busqueda_categorias_async', tarea.id, direcciones, categorias, tipo_regio, headless)
            messages.success(request, f"Tarea #{tarea.id} creada. Procesando {len(categorias)} categorias en {len(direcciones)} direcciones.")
            return redirect('detalle_tarea_catalogacion', pk=tarea.id)
        else:
            return render(request, template, {'form': form})
    else:
        form = BusquedaCategoriasForm()
    return render(request, template, {'form': form})


def sellers_externos_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/sellersExternos.html'
    if request.method == 'POST':
        form = SellersExternosForm(request.POST, request.FILES)
        if form.is_valid():
            archivo = request.FILES['archivo_csv']
            try:
                stream = archivo.read().decode('utf-8').splitlines()
                reader = csv.reader(stream)
                next(reader, None)
                colecciones = [row[0].strip() for row in reader if row and row[0].strip()]
            except Exception as e:
                messages.error(request, f"Error leyendo CSV: {e}")
                return render(request, template, {'form': form})

            if not colecciones:
                messages.error(request, "El archivo no contiene colecciones.")
                return render(request, template, {'form': form})

            headless = not form.cleaned_data.get('ver_navegador', False)

            tarea = TareaCatalogacion.objects.create(
                tipo=TareaCatalogacion.TipoTarea.SELLERS_EXTERNOS,
                progreso_total=len(colecciones)
            )
            async_task('core.tasks.sellers_externos_async', tarea.id, colecciones, headless)
            messages.success(request, f"Tarea #{tarea.id} creada. Procesando {len(colecciones)} colecciones.")
            return redirect('detalle_tarea_catalogacion', pk=tarea.id)
        else:
            return render(request, template, {'form': form})
    else:
        form = SellersExternosForm()
    return render(request, template, {'form': form})


def sellers_no_carrefour_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/sellersNoCarrefour.html'
    if request.method == 'POST':
        form = SellersNoCarrefourForm(request.POST, request.FILES)
        if form.is_valid():
            archivo = request.FILES['archivo_csv']
            try:
                df = pd.read_csv(archivo)
                diccionario = {}
                for col in ['Fravega', 'Megatone', 'Provincia', 'OnCity']:
                    if col in df.columns:
                        valores = df[col].dropna().astype(str).str.strip().tolist()
                        if valores:
                            diccionario[col] = valores
            except Exception as e:
                messages.error(request, f"Error leyendo CSV: {e}")
                return render(request, template, {'form': form})

            if not diccionario:
                messages.error(request, "No se encontraron columnas validas (Fravega, Megatone, Provincia, OnCity).")
                return render(request, template, {'form': form})

            headless = not form.cleaned_data.get('ver_navegador', False)

            total = sum(len(v) for v in diccionario.values())
            tarea = TareaCatalogacion.objects.create(
                tipo=TareaCatalogacion.TipoTarea.SELLERS_NO_CARREFOUR,
                progreso_total=total
            )
            async_task('core.tasks.sellers_no_carrefour_async', tarea.id, diccionario, headless)
            messages.success(request, f"Tarea #{tarea.id} creada. Procesando {total} sellers.")
            return redirect('detalle_tarea_catalogacion', pk=tarea.id)
        else:
            return render(request, template, {'form': form})
    else:
        form = SellersNoCarrefourForm()
    return render(request, template, {'form': form})


def consulta_visibilidad_view(request: HttpRequest) -> HttpResponse:
    template = 'core/Catalogacion/consultaVisibilidad.html'
    if request.method == 'POST':
        form = ConsultaVisibilidadForm(request.POST, request.FILES)
        if form.is_valid():
            tipo = form.cleaned_data['tipo']
            texto = form.cleaned_data.get('valores', '').strip()
            archivo = form.cleaned_data.get('archivo_excel')
            seller = form.cleaned_data['seller']

            # Extraer valores de Excel o texto
            if archivo:
                col_esperada = 'EAN' if tipo == 'ean' else 'SKU'
                valores, error = _leer_columna_excel(archivo, col_esperada)
                if error:
                    messages.error(request, error)
                    return render(request, template, {'form': form, 'active_tab': tipo})
            else:
                valores = [line.strip() for line in texto.splitlines() if line.strip()]

            if not valores:
                messages.error(request, "No se encontraron valores para consultar.")
                return render(request, template, {'form': form, 'active_tab': tipo})

            tarea = TareaCatalogacion.objects.create(
                tipo=TareaCatalogacion.TipoTarea.CONSULTA_VISIBILIDAD,
                progreso_total=len(valores)
            )

            if tipo == 'ean':
                async_task('core.tasks.consulta_visibilidad_ean_async', tarea.id, valores, seller.id)
                etiqueta = 'EANs'
            else:
                async_task('core.tasks.consulta_visibilidad_async', tarea.id, valores, seller.id)
                etiqueta = 'SKUs'

            messages.success(request, f"Tarea #{tarea.id} creada. Consultando visibilidad de {len(valores)} {etiqueta}.")
            return redirect('detalle_tarea_catalogacion', pk=tarea.id)
        else:
            tipo = request.POST.get('tipo', 'ean')
            return render(request, template, {'form': form, 'active_tab': tipo})
    else:
        form = ConsultaVisibilidadForm()
    return render(request, template, {'form': form})


def _leer_columna_excel(archivo, columna_esperada: str) -> tuple[list[str], str | None]:
    """Lee una columna de un Excel. Retorna (valores, error)."""
    try:
        df = pd.read_excel(archivo)
        columna = None
        for col in df.columns:
            if col.strip().upper() == columna_esperada.upper():
                columna = col
                break
        if columna is None:
            return [], f"El archivo no contiene una columna '{columna_esperada}'."
        valores = [str(v).strip() for v in df[columna].dropna().tolist() if str(v).strip()]
        return valores, None
    except Exception as e:
        return [], f"Error leyendo Excel: {e}"


# ── Descarga de plantillas de ejemplo ──────────────────────────────────

PLANTILLAS: dict[str, dict] = {
    'janis': {
        'formato': 'xlsx',
        'nombre': 'plantilla_janis.xlsx',
        'columnas': ['numero_pedido', 'numero_transaccion', 'fecha_hora', 'medio_pago', 'seller', 'estado'],
    },
    'modal_logistica': {
        'formato': 'xlsx',
        'nombre': 'plantilla_modal_logistica.xlsx',
        'columnas': ['skuid', 'modal logistica'],
    },
    'busqueda_eans': {
        'formato': 'csv',
        'nombre': 'plantilla_eans.csv',
        'columnas': ['ean'],
    },
    'busqueda_categorias': {
        'formato': 'csv',
        'nombre': 'plantilla_categorias.csv',
        'columnas': ['categoria'],
    },
    'sellers_externos': {
        'formato': 'csv',
        'nombre': 'plantilla_sellers_externos.csv',
        'columnas': ['numero_coleccion'],
    },
    'sellers_no_carrefour': {
        'formato': 'csv',
        'nombre': 'plantilla_sellers_no_carrefour.csv',
        'columnas': ['Fravega', 'Megatone', 'Provincia', 'OnCity'],
    },
    'visibilidad_ean': {
        'formato': 'xlsx',
        'nombre': 'plantilla_visibilidad_ean.xlsx',
        'columnas': ['EAN'],
    },
    'visibilidad_sku': {
        'formato': 'xlsx',
        'nombre': 'plantilla_visibilidad_sku.xlsx',
        'columnas': ['SKU'],
    },
}


def descargar_plantilla(request: HttpRequest, tipo: str) -> HttpResponse:
    config = PLANTILLAS.get(tipo)
    if not config:
        raise Http404("Plantilla no encontrada")

    if config['formato'] == 'csv':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{config["nombre"]}"'
        writer = csv.writer(response)
        writer.writerow(config['columnas'])
        return response

    # xlsx
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.append(config['columnas'])
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    response = HttpResponse(
        buffer.getvalue(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
    response['Content-Disposition'] = f'attachment; filename="{config["nombre"]}"'
    return response
