function main() {
  var SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/12drOHsoXL_KSPQcD-AoLw4AyJO-bo-hMhGl5CDLtE7g/edit?usp=sharing';
  var spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var sheet = spreadsheet.getActiveSheet();

  var report = AdsApp.report(
      "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros, \
        campaign.name, expanded_landing_page_view.expanded_final_url \
      FROM expanded_landing_page_view WHERE segments.date DURING LAST_7_DAYS"
  ); 

  report.exportToSheet(sheet);
}