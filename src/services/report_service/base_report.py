import os
import datetime
from io import BytesIO

from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    PageBreak,
    Table,
    TableStyle,
    HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY


# ============================================================
# KPMG THEME COLORS & STYLING
# ============================================================
KPMG_BLUE = colors.HexColor("#00338D")
KPMG_LIGHT_BLUE = colors.HexColor("#4CA8E8")
LIGHT_GREY = colors.HexColor("#F0F3F8")
DARK_GREY = colors.HexColor("#4D4D4F")
ACCENT_GREEN = colors.HexColor("#00A651")
WHITE = colors.white
BLACK = colors.black


class BaseReport:
    """Enhanced base class for all PDF report types with premium KPMG styling."""

    def __init__(self, title: str, subtitle: str = "", logo_path: str = None):
        self.title = title
        self.subtitle = subtitle
        self.logo_path = logo_path
        self.styles = getSampleStyleSheet()
        self._create_custom_styles()
        self.output_dir = "reports"

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    # =======================================================================
    # CUSTOM PARAGRAPH STYLES
    # =======================================================================
    def _create_custom_styles(self):
        """Create custom paragraph styles for enhanced aesthetics."""
        
        # Helper function to add style if it doesn't exist
        def add_style_if_not_exists(style):
            if style.name not in self.styles:
                self.styles.add(style)
        
        add_style_if_not_exists(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=36,
            textColor=KPMG_BLUE,
            spaceAfter=6,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            leading=42,
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='CustomSubtitle',
            fontSize=14,
            textColor=DARK_GREY,
            spaceAfter=20,
            alignment=TA_CENTER,
            fontName='Helvetica',
            leading=16,
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='SectionHeading',
            fontSize=16,
            textColor=WHITE,
            textBackgroundColor=KPMG_BLUE,
            spaceAfter=10,
            spaceBefore=10,
            fontName='Helvetica-Bold',
            leading=20,
            leftIndent=10,
            rightIndent=10,
            topPadding=8,
            bottomPadding=8,
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='ChartTitle',
            fontSize=13,
            textColor=KPMG_BLUE,
            spaceAfter=8,
            fontName='Helvetica-Bold',
            leading=15,
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='TableTitle',
            fontSize=13,
            textColor=KPMG_BLUE,
            spaceAfter=8,
            fontName='Helvetica-Bold',
            leading=15,
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='BodyText',
            fontSize=11,
            textColor=DARK_GREY,
            alignment=TA_JUSTIFY,
            spaceAfter=12,
            leading=16,
            fontName='Helvetica',
        ))

        add_style_if_not_exists(ParagraphStyle(
            name='Emphasis',
            fontSize=11,
            textColor=KPMG_LIGHT_BLUE,
            fontName='Helvetica-Bold',
        ))

    # =======================================================================
    # EXPORT PDF
    # =======================================================================
    def export(self, story):
        buffer = BytesIO()

        pdf = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            title=self.title,
            leftMargin=45,
            rightMargin=45,
            topMargin=100,
            bottomMargin=70,
        )

        pdf.build(
            story,
            onFirstPage=self._add_title_page_decoration,
            onLaterPages=self._add_page_decoration,
        )

        buffer.seek(0)
        return buffer

    # =======================================================================
    # PAGE DECORATIONS
    # =======================================================================
    def _add_title_page_decoration(self, canvas_obj, doc):
        """Decorations for title page only."""
        canvas_obj.saveState()
        
        # Top accent bar
        canvas_obj.setFillColor(KPMG_BLUE)
        canvas_obj.rect(0, A4[1] - 50, A4[0], 50, fill=True, stroke=False)

        # Add logo if provided
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                canvas_obj.drawImage(self.logo_path, 45, A4[1] - 45, width=40, height=40)
            except Exception as e:
                print(f"Error drawing logo: {e}")

        canvas_obj.restoreState()

    def _add_page_decoration(self, canvas_obj, doc):
        """Header and footer for all pages."""
        canvas_obj.saveState()

        # Top blue accent bar
        canvas_obj.setFillColor(KPMG_BLUE)
        canvas_obj.rect(0, A4[1] - 40, A4[0], 40, fill=True, stroke=False)

        # Add logo if provided
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                canvas_obj.drawImage(self.logo_path, 45, A4[1] - 37, width=30, height=30)
                title_x = 85
            except Exception as e:
                print(f"Error drawing logo: {e}")
                title_x = 45
        else:
            title_x = 45

        # Header title
        canvas_obj.setFillColor(WHITE)
        canvas_obj.setFont("Helvetica-Bold", 12)
        canvas_obj.drawString(title_x, A4[1] - 22, self.title)

        # Page number in header
        canvas_obj.setFont("Helvetica", 9)
        canvas_obj.drawRightString(A4[0] - 45, A4[1] - 22, f"Page {doc.page}")

        # Footer line (accent color)
        canvas_obj.setFillColor(ACCENT_GREEN)
        canvas_obj.rect(45, 50, A4[0] - 90, 1.5, fill=True, stroke=False)

        # Footer text
        canvas_obj.setFillColor(DARK_GREY)
        canvas_obj.setFont("Helvetica", 8)
        footer_text = "Generated by KPMG Milk Intelligence Platform | Confidential"
        canvas_obj.drawString(45, 30, footer_text)

        # Timestamp
        canvas_obj.drawRightString(A4[0] - 45, 30, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))

        canvas_obj.restoreState()

    # =======================================================================
    # TITLE PAGE (Premium Design)
    # =======================================================================
    def add_title_page(self, story, logo_path=None):
        """Create an elegant title page."""
        
        # Top spacing
        story.append(Spacer(1, 1.2 * inch))

        # Main title
        story.append(Paragraph(self.title, self.styles['CustomTitle']))
        
        # Subtitle if provided
        if self.subtitle:
            story.append(Paragraph(self.subtitle, self.styles['CustomSubtitle']))
        
        story.append(Spacer(1, 0.3 * inch))

        # Decorative line
        hr = HRFlowable(width="40%", thickness=2, color=KPMG_BLUE, spaceAfter=0.3 * inch)
        story.append(hr)

        story.append(Spacer(1, 0.4 * inch))

        # Date and metadata
        date_str = datetime.datetime.now().strftime("%B %d, %Y")
        metadata = f"<font size=11 color='{DARK_GREY.hexval()}'>Report Generated: <b>{date_str}</b></font>"
        story.append(Paragraph(metadata, self.styles['Normal']))

        story.append(Spacer(1, 1.5 * inch))

        # Footer tagline
        tagline = "<font size=10 color='{}'>Advanced Dairy Quality Analytics & Intelligence</font>".format(
            KPMG_LIGHT_BLUE.hexval()
        )
        story.append(Paragraph(tagline, self.styles['CustomSubtitle']))

        story.append(PageBreak())

    # =======================================================================
    # ADD SECTION HEADER
    # =======================================================================
    def add_section_header(self, story, title):
        """Add a styled section header."""
        header = Paragraph(title, self.styles['SectionHeading'])
        story.append(header)
        story.append(Spacer(1, 0.15 * inch))

    # =======================================================================
    # ADD CHART (Enhanced)
    # =======================================================================
    def add_chart(self, story, title, img_path, description=""):
        """Add a chart with title and optional description."""
        
        self.add_section_header(story, title)

        if img_path and os.path.exists(img_path):
            img = Image(img_path, width=6.5 * inch, height=3.5 * inch)
            story.append(img)
        else:
            story.append(Paragraph(
                "<font color='{}' size=11><i>Chart not available</i></font>".format(DARK_GREY.hexval()),
                self.styles['Normal']
            ))

        if description:
            story.append(Spacer(1, 0.15 * inch))
            story.append(Paragraph(description, self.styles['BodyText']))

        story.append(Spacer(1, 0.35 * inch))

    # =======================================================================
    # ADD TABLE (Premium Styling)
    # =======================================================================
    def add_table(self, story, title, data, description=""):
        """Add a beautifully styled table."""
        
        self.add_section_header(story, title)

        if description:
            story.append(Paragraph(description, self.styles['BodyText']))
            story.append(Spacer(1, 0.1 * inch))

        table = Table(data, hAlign='LEFT')
        
        table.setStyle(TableStyle([
            # Header row styling
            ('BACKGROUND', (0, 0), (-1, 0), KPMG_BLUE),
            ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),

            # Body rows
            ('BACKGROUND', (0, 1), (-1, -1), LIGHT_GREY),
            ('TEXTCOLOR', (0, 1), (-1, -1), DARK_GREY),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),

            # Alternating row colors
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [LIGHT_GREY, WHITE]),

            # Grid
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor("#E0E0E0")),
            ('LINEABOVE', (0, 0), (-1, 0), 2, KPMG_BLUE),
            ('LINEBELOW', (0, -1), (-1, -1), 2, KPMG_BLUE),
        ]))

        story.append(table)
        story.append(Spacer(1, 0.35 * inch))

    # =======================================================================
    # ADD NARRATIVE (Premium Styling)
    # =======================================================================
    def add_narrative(self, story, title, text, is_ai=False):
        """Add a narrative section with optional AI indicator."""
        
        header_title = title
        if is_ai:
            header_title += " <font size=9 color='{}'>[AI-Generated]</font>".format(ACCENT_GREEN.hexval())
        
        self.add_section_header(story, header_title)

        cleaned = self._ensure_text(text)
        cleaned = cleaned.replace("\n", "<br/><br/>")
        
        story.append(Paragraph(cleaned, self.styles['BodyText']))
        story.append(Spacer(1, 0.35 * inch))

    # =======================================================================
    # ADD KEY METRICS BOX
    # =======================================================================
    def add_metrics_box(self, story, metrics_dict):
        """Add a highlighted metrics box (key findings)."""
        
        story.append(Spacer(1, 0.3 * inch))
        
        # Create a 2-column table for metrics
        metrics_data = []
        items = list(metrics_dict.items())
        
        for i in range(0, len(items), 2):
            row = []
            # First metric
            metric_name, metric_value = items[i]
            row.append(Paragraph(
                f"<b>{metric_name}</b><br/><font size=14 color='{KPMG_LIGHT_BLUE.hexval()}'>{metric_value}</font>",
                self.styles['Normal']
            ))
            
            # Second metric (if exists)
            if i + 1 < len(items):
                metric_name, metric_value = items[i + 1]
                row.append(Paragraph(
                    f"<b>{metric_name}</b><br/><font size=14 color='{KPMG_LIGHT_BLUE.hexval()}'>{metric_value}</font>",
                    self.styles['Normal']
                ))
            else:
                row.append(Paragraph("", self.styles['Normal']))
            
            metrics_data.append(row)

        metrics_table = Table(metrics_data, colWidths=[3.25 * inch, 3.25 * inch])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), LIGHT_GREY),
            ('TEXTCOLOR', (0, 0), (-1, -1), DARK_GREY),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('PADDING', (0, 0), (-1, -1), 15),
            ('GRID', (0, 0), (-1, -1), 1, KPMG_LIGHT_BLUE),
        ]))

        story.append(metrics_table)
        story.append(Spacer(1, 0.5 * inch))

    # =======================================================================
    # CLEAN TEXT FOR PDF
    # =======================================================================
    def _ensure_text(self, narrative):
        import json

        if narrative is None:
            return "No narrative available."

        if isinstance(narrative, str):
            return narrative

        if isinstance(narrative, dict):
            return json.dumps(narrative, indent=2)

        if isinstance(narrative, list):
            return "\n".join([f"• {item}" for item in narrative])

        return str(narrative)