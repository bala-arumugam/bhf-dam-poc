(function ($, $document) {
    "use strict";

    var _ = window._,
        initialized = false,
        REPORT_TYPE = "duplicateassetsreport",
        EXPORT_REQ_URL = "/apps/bhfdampoc/duplicate-assets.bhfdamreport.json",
        REPORT_TYPE_DETAIL = "Duplicate Assets",
        REPORT_WIZARD = "/mnt/overlay/dam/gui/content/reports/createreportwizard.html";

    if (!isReportWizard()) {
        return;
    }

    init();

    function init(){
        if(initialized){
            return;
        }

        initialized = true;

        $(document).one("foundation-contentloaded.cq-damadmin-createreport-wizard", function(e){
            $(".foundation-wizard", e.target).on("foundation-wizard-stepchange", setExportUrl);
        });
    }

    function setExportUrl(e, to){
        if( (getStepNumber() != 3) || (REPORT_TYPE !== getReportType())){
            return;
        }

        var $lastStep = $(to);

        $lastStep.on("foundation-contentloaded",function(){
            $(".cq-dam-assetthumbnail").find("coral-card-title").html(REPORT_TYPE_DETAIL);
            $("form").prop("action", EXPORT_REQ_URL);
        });
    }

    function getStepNumber(){
        var $wizard = $(".foundation-wizard"),
            currentStep = $wizard.find(".foundation-wizard-step-active"),
            wizardApi = $wizard.adaptTo("foundation-wizard");

        return wizardApi.getPrevSteps(currentStep).length + 1;
    }

    function getReportType(){
        var reportPath = $("input[name='dam-asset-report-type'] ").val();
        return reportPath.substring(reportPath.lastIndexOf("/") + 1);
    }

    function isReportWizard() {
        return (window.location.pathname.indexOf(REPORT_WIZARD) >= 0);
    }
}(jQuery, jQuery(document)));