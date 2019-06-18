from ..models import EDINETCompany, EDINETDocument


class EDINETDocumentRegister():

    @classmethod
    def register_document(self, document, xbrl_path, pdf_path):
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
        try:
            parent = EDINETDocument.objects.get(
                        edinet_document_id=document.document_id)
        except EDINETDocument.DoesNotExist:
            parent = None

        # Register Company's document
        _document = EDINETDocument(
            company=company,
            period_start=document.period_start,
            period_end=document.period_end,
            submitted_date=document.submitted_date,
            document_type=document.doc_type_code,
            lang="ja",
            xbrl_path=xbrl_path,
            pdf_path=pdf_path,
            edinet_document_id=document.document_id,
            title=document.title,
            ordinance_code=document.ordinance_code,
            form_code=document.form_code,
            issuer_edinet_code=document.issuer_edinet_code,
            subject_edinet_code=document.subject_edinet_code,
            subsidiary_edinet_code=document.subsidiary_edinet_code,
            submit_reason=document.submit_reason,
            parent_document_id=parent,
            operated_date=document.operated_date,
            withdraw_status=document.withdraw_status,
            operation_status=document.operation_status,
            disclosure_status=document.disclosure_status,
            has_attachment=document.has_attachment,
            has_xbrl=document.has_xbrl,
            has_pdf=document.has_pdf,
            has_english_doc=document.has_english_doc
        )

        _document.save()
        return _document
