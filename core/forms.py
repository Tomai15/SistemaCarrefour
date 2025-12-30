from django import forms
from django.core.exceptions import ValidationError
from datetime import date
from core.models import UsuarioPayway, UsuarioCDP, ReporteVtex, ReportePayway, ReporteCDP, ReporteJanis


class RangoFechasFormMixin:
    """
    Mixin para formularios que requieren rango de fechas.

    Proporciona campos de fecha_inicio y fecha_fin con validación estándar.
    Sigue el principio DRY evitando duplicar código de fechas.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Configurar campo fecha_inicio si existe
        if 'fecha_inicio' in self.fields:
            self.fields['fecha_inicio'].widget.attrs.update({
                'type': 'date',
                'class': 'form-control form-control-lg',
                'placeholder': 'Seleccione fecha de inicio'
            })

        # Configurar campo fecha_fin si existe
        if 'fecha_fin' in self.fields:
            self.fields['fecha_fin'].widget.attrs.update({
                'type': 'date',
                'class': 'form-control form-control-lg',
                'placeholder': 'Seleccione fecha de fin'
            })

    def clean(self):
        """Validación estándar de rango de fechas."""
        cleaned_data = super().clean()
        fecha_inicio = cleaned_data.get('fecha_inicio')
        fecha_fin = cleaned_data.get('fecha_fin')

        if fecha_inicio and fecha_fin:
            # Validar que fecha_inicio no sea mayor a fecha_fin
            if fecha_inicio > fecha_fin:
                raise ValidationError(
                    'La fecha de inicio no puede ser posterior a la fecha de fin.'
                )

            # Validar que las fechas no sean futuras
            hoy = date.today()
            if fecha_inicio > hoy:
                raise ValidationError(
                    'La fecha de inicio no puede ser una fecha futura.'
                )

            if fecha_fin > hoy:
                raise ValidationError(
                    'La fecha de fin no puede ser una fecha futura.'
                )

        return cleaned_data


class GenerarReportePaywayForm(forms.Form):
    """Formulario para generar un reporte de Payway."""

    fecha_inicio = forms.DateField(

        label='Fecha de Inicio',

        widget=forms.DateInput(attrs={

            'type': 'date',

            'class': 'form-control form-control-lg',

            'placeholder': 'Seleccione fecha de inicio'

        }),

        help_text='Fecha desde la cual se generará el reporte'

    )

    fecha_fin = forms.DateField(

        label='Fecha de Fin',

        widget=forms.DateInput(attrs={

            'type': 'date',

            'class': 'form-control form-control-lg',

            'placeholder': 'Seleccione fecha de fin'

        }),

        help_text='Fecha hasta la cual se generará el reporte'

    )

    def clean(self):

        """Validación personalizada del formulario."""

        cleaned_data = super().clean()

        fecha_inicio = cleaned_data.get('fecha_inicio')

        fecha_fin = cleaned_data.get('fecha_fin')

        if fecha_inicio and fecha_fin:

        # Validar que fecha_inicio no sea mayor a fecha_fin

            if fecha_inicio > fecha_fin:

                raise ValidationError(

                    'La fecha de inicio no puede ser posterior a la fecha de fin.'

                )

            # Validar que las fechas no sean futuras

            hoy = date.today()

            if fecha_inicio > hoy:

                raise ValidationError(

                    'La fecha de inicio no puede ser una fecha futura.'

                )

            if fecha_fin > hoy:

                raise ValidationError(

                    'La fecha de fin no puede ser una fecha futura.'

                )

        return cleaned_data


class CredencialesFormBase(forms.ModelForm):
    """
    Formulario base para credenciales (DRY).

    Proporciona configuración común para formularios de credenciales
    de diferentes plataformas (Payway, CDP, etc.).
    """

    class Meta:
        abstract = True
        fields = ['usuario', 'clave']
        widgets = {
            'usuario': forms.TextInput(attrs={
                'class': 'form-control form-control-lg',
            }),
            'clave': forms.PasswordInput(attrs={
                'class': 'form-control form-control-lg',
            })
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Personalizar placeholders según el nombre de la plataforma
        plataforma = self._get_nombre_plataforma()

        self.fields['usuario'].widget.attrs['placeholder'] = f'Ingrese su usuario de {plataforma}'
        self.fields['clave'].widget.attrs['placeholder'] = f'Ingrese su contraseña de {plataforma}'

        self.fields['usuario'].help_text = f'Usuario para acceder a la plataforma {plataforma}'
        self.fields['clave'].help_text = f'Contraseña asociada a su cuenta de {plataforma}'

    def _get_nombre_plataforma(self):
        """Obtiene el nombre de la plataforma desde el modelo."""
        # Por defecto, usar el nombre del modelo sin "Usuario"
        model_name = self.Meta.model.__name__
        return model_name.replace('Usuario', '')


class CredencialesPaywayForm(CredencialesFormBase):
    """Formulario para editar credenciales de Payway."""

    class Meta(CredencialesFormBase.Meta):
        model = UsuarioPayway
        labels = {
            'usuario': 'Usuario Payway',
            'clave': 'Contraseña Payway'
        }


class CredencialesCDPForm(CredencialesFormBase):
    """Formulario para editar credenciales de CDP."""

    class Meta(CredencialesFormBase.Meta):
        model = UsuarioCDP
        labels = {
            'usuario': 'Usuario CDP',
            'clave': 'Contraseña CDP'
        }


class GenerarReporteVtexForm(RangoFechasFormMixin, forms.Form):
    """Formulario para generar un reporte de VTEX."""

    fecha_inicio = forms.DateField(
        label='Fecha de Inicio',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de inicio'
        }),
        help_text='Fecha desde la cual se generará el reporte'
    )

    fecha_fin = forms.DateField(
        label='Fecha de Fin',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de fin'
        }),
        help_text='Fecha hasta la cual se generará el reporte'
    )


class GenerarReporteCDPForm(RangoFechasFormMixin, forms.Form):
    """Formulario para generar un reporte de CDP."""

    fecha_inicio = forms.DateField(
        label='Fecha de Inicio',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de inicio'
        }),
        help_text='Fecha desde la cual se generará el reporte'
    )

    fecha_fin = forms.DateField(
        label='Fecha de Fin',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de fin'
        }),
        help_text='Fecha hasta la cual se generará el reporte'
    )


class GenerarReporteJanisForm(RangoFechasFormMixin, forms.Form):
    """Formulario para generar un reporte de Janis."""

    fecha_inicio = forms.DateField(
        label='Fecha de Inicio',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de inicio'
        }),
        help_text='Fecha desde la cual se generará el reporte'
    )

    fecha_fin = forms.DateField(
        label='Fecha de Fin',
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'form-control form-control-lg',
            'placeholder': 'Seleccione fecha de fin'
        }),
        help_text='Fecha hasta la cual se generará el reporte'
    )


class GenerarCruceForm(forms.Form):
    """
    Formulario para generar un cruce de reportes.

    Permite seleccionar reportes de diferentes tipos (VTEX, Payway, CDP).
    Requiere al menos 2 reportes de diferentes tipos.
    """

    reporte_vtex = forms.ModelChoiceField(
        queryset=ReporteVtex.objects.filter(estado='COMPLETADO').order_by('-id'),
        required=False,
        empty_label="-- Ninguno --",
        widget=forms.Select(attrs={
            'class': 'form-control form-control-lg'
        })
    )

    reporte_payway = forms.ModelChoiceField(
        queryset=ReportePayway.objects.filter(estado='COMPLETADO').order_by('-id'),
        required=False,
        empty_label="-- Ninguno --",
        widget=forms.Select(attrs={
            'class': 'form-control form-control-lg'
        })
    )

    reporte_cdp = forms.ModelChoiceField(
        queryset=ReporteCDP.objects.filter(estado='COMPLETADO').order_by('-id'),
        required=False,
        empty_label="-- Ninguno --",
        widget=forms.Select(attrs={
            'class': 'form-control form-control-lg'
        })
    )

    reporte_janis = forms.ModelChoiceField(
        queryset=ReporteJanis.objects.filter(estado='COMPLETADO').order_by('-id'),
        required=False,
        empty_label="-- Ninguno --",
        widget=forms.Select(attrs={
            'class': 'form-control form-control-lg'
        })
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Personalizar las etiquetas de los reportes para mostrar más información
        self.fields['reporte_vtex'].label_from_instance = lambda obj: f"#{obj.id} - {obj.fecha_inicio} a {obj.fecha_fin}"
        self.fields['reporte_payway'].label_from_instance = lambda obj: f"#{obj.id} - {obj.fecha_inicio} a {obj.fecha_fin}"
        self.fields['reporte_cdp'].label_from_instance = lambda obj: f"#{obj.id} - {obj.fecha_inicio} a {obj.fecha_fin}"
        self.fields['reporte_janis'].label_from_instance = lambda obj: f"#{obj.id} - {obj.fecha_inicio} a {obj.fecha_fin}"

    def clean(self):
        """Validar que se seleccionen al menos 2 reportes de diferentes tipos."""
        cleaned_data = super().clean()

        reporte_vtex = cleaned_data.get('reporte_vtex')
        reporte_payway = cleaned_data.get('reporte_payway')
        reporte_cdp = cleaned_data.get('reporte_cdp')
        reporte_janis = cleaned_data.get('reporte_janis')

        # Contar cuántos reportes se seleccionaron
        reportes_seleccionados = sum([
            1 if reporte_vtex else 0,
            1 if reporte_payway else 0,
            1 if reporte_cdp else 0,
            1 if reporte_janis else 0,
        ])

        if reportes_seleccionados < 2:
            raise ValidationError(
                'Debe seleccionar al menos 2 reportes de diferentes tipos para generar el cruce.'
            )

        return cleaned_data
