from fastapi import APIRouter, Response
from src.services.report_service.daily_report import DailyQualityReport
from src.services.report_service.weekly_trends_report import WeeklyCompositionReport
from src.services.report_service.route_performance_report import RoutePerformanceReport
from src.services.report_service.supplier_scorecard_report import SupplierScorecardReport
from src.services.report_service.monthly_adulteration_report import MonthlyAdulterationReport
from src.services.report_service.shap_analysis_report import ShapAnalysisReport

router = APIRouter(prefix="/reports", tags=["Reports"])


@router.get("/daily")
def generate_daily_report():
    try:
        pdf_buffer = DailyQualityReport().build()   # BytesIO
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": 'attachment; filename="daily_report.pdf"'
            },
        )
    except Exception as e:
        return {"error": str(e)}


@router.get("/weekly")
def generate_weekly_report():
    try:
        pdf_buffer = WeeklyCompositionReport().build()
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": 'attachment; filename="weekly_trends_report.pdf"'
            },
        )
    except Exception as e:
        return {"error": str(e)}


@router.get("/route")
def generate_route_report():
    try:
        pdf_buffer = RoutePerformanceReport().build()
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": 'attachment; filename="route_performance_report.pdf"'
            },
        )
    except Exception as e:
        return {"error": str(e)}


@router.get("/suppliers")
def generate_supplier_scorecard_report():
    try:
        pdf_buffer = SupplierScorecardReport().build()
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": 'attachment; filename="supplier_scorecard_report.pdf"'
            },
        )
    except Exception as e:
        return {"error": str(e)}

@router.get("/monthly/adulteration")
def generate_monthly_report():
    try:
        pdf_buffer = MonthlyAdulterationReport().build()
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={"Content-Disposition": 'attachment; filename="monthly_adulteration_report.pdf"'}
        )
    except Exception as e:
        return {"error": str(e)}
    
@router.get("/shap")
def generate_shap_report():
    try:
        pdf_buffer = ShapAnalysisReport().build()
        return Response(
            content=pdf_buffer.getvalue(),
            media_type="application/pdf",
            headers={
                "Content-Disposition": 'attachment; filename="shap_analysis_report.pdf"'
            }
        )
    except Exception as e:
        return {"error": str(e)}
