from django.db import models
from .masters import Company, Document


class Feature(models.Model):
    company = models.ForeignKey(Company, on_delete=models.PROTECT)
    year_month = models.DateField()
    unit = models.CharField(max_length=3)
    document = models.ForeignKey(Document, on_delete=models.PROTECT)
    base = models.TextField()

    class Meta:
        abstract = True


class NumberOfExecutives(Feature):
    value = models.IntegerField()
