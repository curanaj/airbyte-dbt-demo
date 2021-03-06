{{ config(alias="exchange_rate", schema="test_normalization", tags=["top-level"]) }}
-- Final base SQL model
select
    {{ adapter.quote('id') }},
    currency,
    {{ adapter.quote('date') }},
    {{ adapter.quote('HKD@spéçiäl & characters') }},
    hkd_special___characters,
    nzd,
    usd,
    _airbyte_emitted_at,
    _airbyte_exchange_rate_hashid
from {{ ref('exchange_rate_ab3_e8c') }}
-- exchange_rate from {{ source('test_normalization', '_airbyte_raw_exchange_rate') }}

