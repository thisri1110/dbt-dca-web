
{{ config(
    materialized = 'table',
    cluster_by = ['visitDate'],
    tags = ['mds-dca-web-production-daily_event_scrum', 'mds-dca-web-production-intraday_event_scrum', 'mds-dca-web-ask-astro_starter_event_scrum']
) }}

WITH source AS (
    SELECT
        x.visit_date AS visitDate,
        x.client_id AS clientID,
        x.session_id AS sessionID,
        x.bounce_flag AS bounceFlag,
        x.active_session_flag AS activeSessionFlag,
        x.internal_user_flag AS internalUserFlag,
        x.page_name AS pageName,
        x.page_url AS pageURL,
        x.element_name AS elementName,
        x.element_type AS elementType,
        x.message_id AS messageID,
        x.chat_id AS chatID,
        x.click_url AS clickURL,
        x.event_code AS eventCode,
        x.click_text AS clickText,
        x.geo_country AS geoCountry,
        x.tbid_role AS tbidRole,
        x.taxonomy_role AS taxonomyRole,
        x.demandbase_company_size AS demandbaseCompanySize,
        x.form_company_size AS formCompanySize,
        x.bid_company AS bidCompany,
        x.demandbase_industry AS demandbaseIndustry,
        x.industry_section AS industrySection,
        x.taxonomy_industry AS taxonomyIndustry,
        x.login_status AS loginStatus,
        x.tbid_auth_status AS tbidAuthStatus,
        x.page_title AS pageTitle,
        x.page_location AS pageLocation,
        x.visitor_category AS visitorCategory,
        SUM(x.page_views) AS pageViews,
        SUM(x.chat_clicks) AS chatClicks,
        SUM(x.chat_closes) AS chatCloses,
        SUM(x.chat_collapses) AS chatCollapses,
        SUM(x.chat_expands) AS chatExpands,
        SUM(x.chat_initiations) AS chatInitiations,
        SUM(x.chat_maximizes) AS chatMaximizes,
        SUM(x.chat_minimizes) AS chatMinimizes,
        SUM(x.chat_opens) AS chatOpens,
        SUM(x.chat_sents) AS chatSents,
        SUM(x.chat_starts) AS chatStarts,
        SUM(x.chat_transcripts) AS chatTranscripts,
        SUM(x.chat_views) AS chatViews,
        MAX(x.audit_etl_job_ins_ts) AS audit_etl_job_ins_ts,
        MAX(x.audit_etl_job_upd_ts) AS audit_etl_job_upd_ts,
       
    FROM sse_dm_mkt_preprd.DM_WEB_ANALYTICS.DM_WEB_ANALYTICS_EVENTS_SCRUM x
   
    WHERE x.bot_flag = FALSE
      AND (x.page_url ILIKE 'https://www.salesforce.com/puls/assistant/%' 
           OR x.page_url ILIKE 'https://reg.salesforce.com/flow/plus/%')
    GROUP BY ALL
    {{ apply_row_limit() }}
)

SELECT * FROM source
