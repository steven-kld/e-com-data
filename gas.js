function pingFastAPI() {
    const url = "https://e-com-data.vercel.app/run-db-update";
    try {
        const response = UrlFetchApp.fetch(url);
        Logger.log("Ping success: " + response.getResponseCode());
    } catch (e) {
        Logger.log("Ping failed: " + e);
    }
}
  