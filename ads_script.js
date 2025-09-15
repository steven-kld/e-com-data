function main() {
  var SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/12drOHsoXL_KSPQcD-AoLw4AyJO-bo-hMhGl5CDLtE7g/edit?usp=sharing';
  var spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var sheet = spreadsheet.getActiveSheet();

  var report = AdsApp.report(
      "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros, \
        campaign.name, expanded_landing_page_view.expanded_final_url \
      FROM expanded_landing_page_view WHERE segments.date DURING LAST_30_DAYS"
  ); 

  report.exportToSheet(sheet);
}

// Fails for some reason
function main() {
  var SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/12drOHsoXL_KSPQcD-AoLw4AyJO-bo-hMhGl5CDLtE7g/edit?usp=sharing';
  var spreadsheet = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  var sheet = spreadsheet.getActiveSheet();

  var endDate = new Date();
  var startDate = new Date();
  
  endDate.setDate(endDate.getDate() - 1); 
  startDate.setDate(startDate.getDate() - 60);

  var startDateString = Utilities.formatDate(startDate, 'GMT', 'yyyy-MM-dd');
  var endDateString = Utilities.formatDate(endDate, 'GMT', 'yyyy-MM-dd');

  var report = AdsApp.report(
      "SELECT segments.date, metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros, " +
      "campaign.name, expanded_landing_page_view.expanded_final_url " +
      "FROM expanded_landing_page_view " +
      `WHERE segments.date BETWEEN '${startDateString}' AND '${endDateString}'`
  );

  report.exportToSheet(sheet);
}