from django.db import models
from django.core.exceptions import ValidationError
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey

class DataPoint(models.Model):
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, null=True, blank=True, default=1)
    object_id = models.PositiveIntegerField(null=True, blank=True, default=1)
    series = GenericForeignKey('content_type', 'object_id')

    date = models.DateField()
    value = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-date']

    def __str__(self):
        return f"{self.series.name} on {self.date}: {self.value}"

    def clean(self):
        if DataPoint.objects.filter(content_type=self.content_type, object_id=self.object_id, date=self.date).exists() and not self.pk:
            raise ValidationError(f"A data point for {self.date} in {self.series} already exists.")

    def save(self, *args, **kwargs):
        self.clean()
        super(DataPoint, self).save(*args, **kwargs)