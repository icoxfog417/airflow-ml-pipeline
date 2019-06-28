from django.db import models
from eagle.models.masters import Document


class Feature(models.Model):
    document = models.ForeignKey(Document, on_delete=models.PROTECT)
    unit = models.CharField(max_length=3)
    ground = models.TextField()

    class Meta:
        abstract = True


class NumberOfExecutives(Feature):
    value = models.IntegerField()
