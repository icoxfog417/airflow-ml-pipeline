from django.db import models


class Company(models.Model):
    local_name = models.TextField()
    global_name = models.TextField()


class EDINETCompany(Company):
    jcn = models.CharField(max_length=13)
    edinet_code = models.CharField(max_length=6)
    sec_code = models.CharField(max_length=5)
    fund_code = models.CharField(max_length=6)


class Document(models.Model):
    company = models.ForeignKey(Company, on_delete=models.PROTECT)
    period_start = models.DateField()
    period_end = models.DateField()
    submitted_date = models.DateTimeField()
    document_type = models.CharField(max_length=3)
    lang = models.CharField(max_length=2)
    xbrl_path = models.TextField()
    pdf_path = models.TextField()


class EDINETDocument(Document):
    edinet_document_id = models.CharField(max_length=8)
    title = models.TextField()
    description = models.TextField()
    ordinance_code = models.CharField(max_length=3)
    form_code = models.CharField(max_length=6)
    issuer_edinet_code = models.CharField(max_length=6)
    subject_edinet_code = models.CharField(max_length=6)
    subsidiary_edinet_code = models.CharField(max_length=6)
    submit_reason = models.TextField()
    parent_document_id = models.ForeignKey("EDINETDocument",
                                           on_delete=models.PROTECT)
    operation_date = models.DateTimeField()
    withdraw_status = models.CharField(max_length=1)
    operation_status = models.CharField(max_length=1)
    disclosure_status = models.CharField(max_length=1)
    has_attached_doc = models.BooleanField()
    has_xbrl_doc = models.BooleanField()
    has_pdf_doc = models.BooleanField()
    has_english_doc = models.BooleanField()
