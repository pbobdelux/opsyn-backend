-- Add crm_app access for Preston Anderson
-- Preston: 7d59993c-3b05-42cb-8077-4370c408e245
-- crm_app: crm_app
-- role: admin

INSERT INTO employee_app_access (id, employee_id, app_id, role, is_active, created_at)
VALUES (
    gen_random_uuid()::text,
    '7d59993c-3b05-42cb-8077-4370c408e245'::text,
    'crm_app'::text,
    'admin'::text,
    true,
    NOW()::timestamptz
)
ON CONFLICT DO NOTHING;
