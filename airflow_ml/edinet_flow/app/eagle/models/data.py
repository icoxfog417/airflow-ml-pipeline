from django.db import models
from eagle.models.masters import Company
import eagle.models.features as F


class CompanyData(models.Model):
    company = models.ForeignKey(Company, on_delete=models.PROTECT)
    year_month = models.DateField()
    number_of_executives = models.ForeignKey(
        F.NumberOfExecutives, on_delete=models.PROTECT)

    class Meta:
        unique_together = ["company", "year_month"]
