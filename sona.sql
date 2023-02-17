WITH shifts AS (

    SELECT * 
        FROM {{ ref('shifts') }}

)

, contracted_hours AS (

    SELECT * 
        FROM {{ ref('contracted_hours') }}

)

, joins AS (

    SELECT
        shifts.id 
        , shifts.start_datetime
        , shifts.end_datetime
        , shifts.shift_type_id
        , shifts.user_id
        , shifts.organisation_id
        , shifts.deleted_at
        , contracted_hours.id as contracted_hours_id
        , CAST(contracted_hours.hours AS FLOAT64) AS hours
        , contracted_hours.description

    FROM
        shifts
    LEFT JOIN 
        contracted_hours 
    ON 
        shifts.user_id = contracted_hours.user_id 
)

, weekly_contracted_hours_cte AS (

    SELECT 
        joins.id 
        , joins.start_datetime
        , joins.contracted_hours_id
        , joins.end_datetime
        , joins.shift_type_id
        , joins.user_id
        , joins.organisation_id
        , joins.deleted_at
        , joins.hours
        , joins.description
        , CASE 
            WHEN joins.description = 'weekly' THEN joins.hours
            WHEN joins.description = '4 weekly' THEN joins.hours / 4
        END AS weekly_contracted_hours 

    FROM 
        joins 
)

, week_label_cte AS (

    SELECT 
        weekly_contracted_hours_cte.id 
        , weekly_contracted_hours_cte.start_datetime
        , weekly_contracted_hours_cte.end_datetime
        , weekly_contracted_hours_cte.shift_type_id
        , weekly_contracted_hours_cte.user_id
        , weekly_contracted_hours_cte.organisation_id
        , weekly_contracted_hours_cte.deleted_at
        , weekly_contracted_hours_cte.contracted_hours_id 
        , weekly_contracted_hours_cte.hours
        , weekly_contracted_hours_cte.description
        , weekly_contracted_hours_cte.weekly_contracted_hours
        , FORMAT_DATE('%Y / %U', DATE_TRUNC(DATE(EXTRACT(DATE FROM start_datetime)), WEEK)) AS week_label
        
        FROM 
            weekly_contracted_hours_cte
)

, hours_worked_week_cte AS (
    SELECT 
        week_label_cte.id 
        , week_label_cte.start_datetime
        , week_label_cte.end_datetime
        , week_label_cte.shift_type_id
        , week_label_cte.user_id
        , week_label_cte.organisation_id
        , week_label_cte.deleted_at
        , week_label_cte.contracted_hours_id 
        , week_label_cte.hours
        , week_label_cte.description
        , week_label_cte.weekly_contracted_hours
        , week_label_cte.week_label
        , SUM(hours) OVER (PARTITION BY user_id, week_label) AS hours_worked_week
    
    FROM 
        week_label 
)

, final_cte AS (

    SELECT 
        id
        , start_datetime
        , end_datetime
        , shift_type_id 
        , user_id 
        , organisation_id 
        , deleted_at 
        , contracted_hours_id
        , hours 
        , description 
        , weekly_contracted_hours 
        , week_label
        , hours_worked_week 
    
    FROM 
        hours_worked_week_cte
    WHERE 
        start_datetime <= CURRENT_TIMESTAMP()
    AND 
        end_datetime <= CURRENT_TIMESTAMP()
)

SELECT * FROM final_cte
