import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2.service_account import Credentials

def init_google_credentials():
    """
    Initializes Google credentials from environment variables for secure access.
    """
    try:
        required_vars = ["GOOGLE_PROJECT_ID", "GOOGLE_PRIVATE_KEY_ID", "GOOGLE_PRIVATE_KEY", "GOOGLE_CLIENT_EMAIL", "GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_X509_CERT_URL"]
        if not all(os.getenv(v) for v in required_vars):
            print("ERROR: One or more required credentials not found in environment variables.")
            return None
            
        service_account_info = {
            "type": "service_account",
            "project_id": os.getenv("GOOGLE_PROJECT_ID"),
            "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),
            "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
        }
        return Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
    except Exception as e:
        print(f"ERROR: Failed to initialize credentials. Check your .env file. Error: {e}")
        return None

credentials = init_google_credentials()
client = BetaAnalyticsDataClient(credentials=credentials) if credentials else None
ga4_property_id = os.getenv('GA4_PROPERTY_ID')

def get_google_ads_report(property_id: str):
    """
    Fetches Google Ads performance data grouped by campaign name.
    """
    if not client or not property_id:
        print("GA4 client is not configured. Please check your environment variables.")
        return None
        
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="googleAdsCampaignName")],
        metrics=[
            Metric(name="advertiserAdImpressions"),
            Metric(name="advertiserAdClicks"),
            Metric(name="advertiserAdCost"),
            # Pay attention to the fact that the values could be inaccurate below this line
            Metric(name="totalPurchasers"),
            Metric(name="purchaseRevenue")
        ],
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")]
    )

    try:
        response = client.run_report(request)
        
        report_data = []
        if response.rows:
            for row in response.rows:
                row_data = {
                    "campaignName": row.dimension_values[0].value,
                    "imps": int(row.metric_values[0].value),
                    "clicks": int(row.metric_values[1].value),
                    "cost": int(round(float(row.metric_values[2].value), 0)),
                    # Pay attention to the fact that the values could be inaccurate below this line
                    "purchases": int(row.metric_values[3].value),
                    "revenue": int(round(float(row.metric_values[4].value), 0))
                }
                report_data.append(row_data)
        
        return report_data

    except Exception as e:
        print(f"An error occurred while fetching data from GA4: {e}")
        return None

if __name__ == '__main__':
    print("Fetching Google Ads performance report for today...")
    ads_report = get_google_ads_report(ga4_property_id)

    if ads_report:
        print(json.dumps(ads_report, indent=2))
    else:
        print("No data found for today or an error occurred.")
