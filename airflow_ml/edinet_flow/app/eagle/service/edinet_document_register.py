from eagle.models import EDINETCompany, EDINETDocument


class EDINETDocumentRegister():

    @classmethod
    def register_document(cls, document, xbrl_path, pdf_path):
        # Confirm company registration
        jcn = document.jcn
        company = None
        try:
            company = EDINETCompany.objects.get(jcn=jcn)
        except EDINETCompany.DoesNotExist:
            company = None

        if company is None:
            company = EDINETCompany(
                local_name=document.filer_name,
                global_name=document.filer_name,
                jcn=document.jcn,
                edinet_code=document.edinet_code,
                sec_code=document.sec_code,
                fund_code=document.fund_code
            )
            company.save()

        parent = None
        if document.parent_document_id:
            try:
                parent = EDINETDocument.objects.get(
                            edinet_document_id=document.parent_document_id)
            except EDINETDocument.DoesNotExist:
                parent = None

        _document = EDINETDocument()
        try:
            _document = EDINETDocument.objects.get(
                        edinet_document_id=document.document_id)
        except EDINETDocument.DoesNotExist:
            _document = EDINETDocument()

        # Register Company's document
        _document.company = company
        _document.period_start = document.period_start
        _document.period_end = document.period_end
        _document.submitted_date = document.submitted_date
        _document.lang = "ja"
        _document.path = xbrl_path
        _document.xbrl_path = xbrl_path
        _document.pdf_path = pdf_path
        _document.edinet_document_id = document.document_id
        _document.edinet_document_type = document.doc_type_code
        _document.title = document.title
        _document.ordinance_code = document.ordinance_code
        _document.form_code = document.form_code
        _document.issuer_edinet_code = document.issuer_edinet_code
        _document.subject_edinet_code = document.subject_edinet_code
        _document.subsidiary_edinet_code = document.subsidiary_edinet_code
        _document.submit_reason = document.submit_reason
        _document.parent_document_id = parent
        _document.operated_date = document.operated_date
        _document.withdraw_status = document.withdraw_status
        _document.operation_status = document.operation_status
        _document.disclosure_status = document.disclosure_status
        _document.has_attachment = document.has_attachment
        _document.has_xbrl = document.has_xbrl
        _document.has_pdf = document.has_pdf
        _document.has_english_doc = document.has_english_doc

        _document.save()
        return _document
