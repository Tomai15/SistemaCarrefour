from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, DetailView
from django.views.generic.detail import SingleObjectMixin
from django.http import FileResponse, Http404
from django.contrib import messages
from django.db.models import Sum, Count, Q
import os

from django_q.tasks import async_task

from core.models import ReportePayway, ReporteVtex, ReporteCDP, ReporteJanis, Cruce, UsuarioPayway, UsuarioCDP
from core.forms import (
    GenerarReportePaywayForm,
    GenerarReporteVtexForm,
    GenerarReporteCDPForm,
    GenerarReporteJanisForm,
    GenerarCruceForm,
    CredencialesPaywayForm,
    CredencialesCDPForm
)
from datetime import date


# Create your views here.
def home(request):
    return render(request, 'core/home.html')

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

    def get(self, request, *args, **kwargs):
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReportePayway.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_excel(request, pk):
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


def generar_reporte_payway_view(request):
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

    def get(self, request, *args, **kwargs):
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteVtex.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_vtex_excel(request, pk):
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


def generar_reporte_vtex_view(request):
    """
    Vista para generar un nuevo reporte de VTEX.

    Muestra un formulario para ingresar fechas y encola la generación del reporte.
    """
    if request.method == 'POST':
        form = GenerarReporteVtexForm(request.POST)

        if form.is_valid():
            # Obtener fechas del formulario
            fecha_inicio = form.cleaned_data['fecha_inicio']
            fecha_fin = form.cleaned_data['fecha_fin']

            # Formatear fechas para el servicio (DD/MM/YYYY)
            fecha_inicio_str = fecha_inicio.strftime("%d/%m/%Y")
            fecha_fin_str = fecha_fin.strftime("%d/%m/%Y")

            # Crear el reporte en estado PENDIENTE
            nuevo_reporte = ReporteVtex.objects.create(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                estado=ReporteVtex.Estado.PENDIENTE
            )

            # Encolar tarea en Django-Q
            try:
                task_id = async_task(
                    'core.tasks.generar_reporte_vtex_async',
                    fecha_inicio_str,
                    fecha_fin_str,
                    nuevo_reporte.id  # Pasar solo el ID, no el objeto completo
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

def ajustes_view(request):
    """
    Vista para gestionar credenciales de Payway y CDP.

    Permite ver y editar las credenciales de ambas plataformas.
    """
    # Obtener o crear instancias de credenciales
    credenciales_payway = UsuarioPayway.objects.first()
    credenciales_cdp = UsuarioCDP.objects.first()

    if request.method == 'POST':
        # Determinar qué formulario se envió
        if 'payway_submit' in request.POST:
            form_payway = CredencialesPaywayForm(request.POST, instance=credenciales_payway)

            if form_payway.is_valid():
                form_payway.save()
                messages.success(request, 'Credenciales de Payway actualizadas exitosamente.')
                return redirect('ajustes')

            # Si hay errores, mantener el otro formulario sin cambios
            form_cdp = CredencialesCDPForm(instance=credenciales_cdp)

        elif 'cdp_submit' in request.POST:
            form_cdp = CredencialesCDPForm(request.POST, instance=credenciales_cdp)

            if form_cdp.is_valid():
                form_cdp.save()
                messages.success(request, 'Credenciales de CDP actualizadas exitosamente.')
                return redirect('ajustes')

            # Si hay errores, mantener el otro formulario sin cambios
            form_payway = CredencialesPaywayForm(instance=credenciales_payway)

        else:
            # No debería llegar aquí, pero por seguridad
            form_payway = CredencialesPaywayForm(instance=credenciales_payway)
            form_cdp = CredencialesCDPForm(instance=credenciales_cdp)

    else:
        # GET request - mostrar formularios
        form_payway = CredencialesPaywayForm(instance=credenciales_payway)
        form_cdp = CredencialesCDPForm(instance=credenciales_cdp)

    context = {
        'form_payway': form_payway,
        'form_cdp': form_cdp,
        'tiene_payway': credenciales_payway is not None,
        'tiene_cdp': credenciales_cdp is not None,
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

    def get(self, request, *args, **kwargs):
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteCDP.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_cdp_excel(request, pk):
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


def generar_reporte_cdp_view(request):
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

    def get(self, request, *args, **kwargs):
        # Obtener el reporte (SingleObjectMixin)
        self.object = self.get_object(queryset=ReporteJanis.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        # Obtener transacciones del reporte actual, ordenadas por fecha descendente
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Agregar el reporte al contexto
        context['reporte'] = self.object
        return context


def exportar_reporte_janis_excel(request, pk):
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


def generar_reporte_janis_view(request):
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


def importar_reporte_janis_view(request):
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

    def get(self, request, *args, **kwargs):
        self.object = self.get_object(queryset=Cruce.objects.all())
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        return self.object.transacciones.all().order_by('-fecha_hora')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['cruce'] = self.object
        return context


def exportar_cruce_excel(request, pk):
    """Vista para exportar un cruce a Excel."""
    cruce = get_object_or_404(Cruce, pk=pk)

    # El modelo es responsable de generar el archivo y retornar su ruta
    ruta_archivo = cruce.generar_reporter_excel()

    if not os.path.exists(ruta_archivo):
        raise Http404("El archivo no se generó correctamente")

    nombre_archivo = os.path.basename(ruta_archivo)

    response = FileResponse(
        open(ruta_archivo, 'rb'),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{nombre_archivo}"'

    return response


def generar_cruce_view(request):
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
