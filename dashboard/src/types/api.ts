export interface DashboardStats {
  cases_total: number
  cases_by_status: Record<string, number>
  emails_total: number
  emails_by_result: Record<string, number>
  documents_total: number
  triage_count: number
}

export interface TriageEmail {
  provider_message_id: string
  from_email: string
  subject: string
  body_text: string
  conversation_id: string
  parsed_result: Record<string, unknown>
  matched_by: string
  processed_at: string
  attachments_count: number
}

export interface CaseListItem {
  case_id: string
  applicant_name: string
  partner_email: string
  status: string
  onedrive_folder_id: string
  last_status_change: string
  missing_financing: string[]
  missing_applicant_data: string[]
  missing_docs_count: number
  total_docs_required: number
  overrides_applied: string[]
  is_complete: boolean
  completeness_pct: number
}

export interface CaseDocument {
  _id: number
  file_name: string
  doc_type: string
  processing_status: string
  processed_at: string
  extracted_fields: string[]
  gdrive_file_id?: string
  gdrive_url?: string
  onedrive_file_id?: string
  onedrive_url?: string
}

export interface CaseEmail {
  subject: string
  from_email: string
  mail_type: string
  processing_result: string
  processed_at: string
  matched_by: string
  body_text: string
  body_html?: string
  parsed_result: Record<string, unknown>
}

export interface AuditEntry {
  event: string
  ts: string
  status?: string
  source?: string
  actor?: string
}

export interface CaseDetail {
  case_id: string
  applicant_name: string
  partner_email: string
  status: string
  onedrive_folder_id: string
  onedrive_web_url: string
  google_drive_links: string[]
  investagon_links: string[]
  last_status_change: string
  europace_case_id: string
  finlink_lead_id: string
  europace_response: Record<string, unknown>
  conversation_ids: string[]
  facts_extracted: Record<string, unknown>
  answers_user: Record<string, unknown>
  manual_overrides: Record<string, unknown>
  readiness: ReadinessResult
  audit_log: AuditEntry[]
  documents: CaseDocument[]
  emails: CaseEmail[]
}

export interface ReadinessResult {
  status: string
  missing_financing: string[]
  missing_applicant_data?: string[]
  missing_docs: MissingDoc[]
  stale_docs: StaleDoc[]
  warnings: string[]
  recommended_missing?: string[]
  manual_overrides_applied: string[]
  effective_view: Record<string, unknown>
}

export interface MissingDoc {
  type: string
  required: number
  found: number
}

export interface StaleDoc {
  type: string
  doc_type?: string
  found?: number
  required?: number
}

export interface OutgoingEmail {
  to: string
  subject: string
  body_text: string
  body_html: string
  logged_at: string
  dry_run: boolean
}

export interface LoginResponse {
  access_token: string
  token_type: string
  user: string
}

export interface OpenAICredits {
  hard_limit_usd?: number
  plan?: string
  used_usd?: number
  fetched_at?: string
  error?: string
}

export interface ApiResponse<T = unknown> {
  success?: boolean
  error?: string
  data?: T
}
