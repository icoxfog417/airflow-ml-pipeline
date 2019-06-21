from django.db import models


class Company(models.Model):
    local_name = models.TextField()
    global_name = models.TextField()


class EDINETCompany(Company):
    jcn = models.CharField(max_length=13)
    edinet_code = models.CharField(max_length=6)
    sec_code = models.CharField(max_length=5, null=True)
    fund_code = models.CharField(max_length=6, null=True)


class Document(models.Model):
    company = models.ForeignKey(Company, on_delete=models.PROTECT)
    period_start = models.DateField(null=True)
    period_end = models.DateField(null=True)
    submitted_date = models.DateTimeField()
    lang = models.CharField(max_length=2)
    xbrl_path = models.TextField()
    pdf_path = models.TextField()


class EDINETDocument(Document):
    edinet_document_id = models.CharField(max_length=8)
    edinet_document_type = models.CharField(max_length=3)
    title = models.TextField()
    ordinance_code = models.CharField(max_length=3)
    form_code = models.CharField(max_length=6)
    issuer_edinet_code = models.CharField(max_length=6, null=True)
    subject_edinet_code = models.CharField(max_length=6, null=True)
    subsidiary_edinet_code = models.CharField(max_length=6, null=True)
    submit_reason = models.TextField(null=True)
    parent_document_id = models.ForeignKey("EDINETDocument",
                                           on_delete=models.PROTECT,
                                           null=True)
    operated_date = models.DateTimeField(null=True)
    withdraw_status = models.CharField(max_length=1)
    operation_status = models.CharField(max_length=1)
    disclosure_status = models.CharField(max_length=1)
    has_attachment = models.BooleanField()
    has_xbrl = models.BooleanField()
    has_pdf = models.BooleanField()
    has_english_doc = models.BooleanField()
