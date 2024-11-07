WITH message_blocks AS (
    -- Определяем блоки сообщений. Группируем подряд идущие сообщения от одного участника диалога.
    SELECT
        message_id,
        type,
        entity_id,
        created_by,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY entity_id, created_by ORDER BY created_at
        ) - ROW_NUMBER() OVER (
            PARTITION BY entity_id ORDER BY created_at
        ) AS block_id
    FROM test.chat_messages
),
first_messages AS (
    -- Оставляем только первое сообщение из каждого блока
    select *
    from (
    	select 
    		message_id,
        	type,
        	entity_id,
        	created_by,
        	created_at,
        	block_id, 
        	ROW_NUMBER() OVER (PARTITION BY entity_id, block_id ORDER BY created_at) AS row_num
    	FROM message_blocks
)AS subquery
WHERE row_num = 1
),
response_times AS (
    -- Рассчитываем время отклика для каждого ответа менеджера на сообщение клиента
    SELECT
        client.message_id AS client_message_id,
        manager.message_id AS manager_message_id,
        client.created_at AS client_created_at,
        manager.created_at AS manager_created_at,
        manager.created_by AS manager_id,
        EXTRACT(EPOCH FROM (to_timestamp(manager.created_at) - to_timestamp(client.created_at))) / 60 AS response_time_minutes
    FROM first_messages client
    JOIN first_messages manager ON client.entity_id = manager.entity_id
    WHERE client.type = 'incoming_chat_message'
    AND manager.type = 'outgoing_chat_message'
    AND manager.created_at > client.created_at
),
adjusted_response_times AS (
    -- Корректируем время отклика с учётом рабочего времени (с 09:30 до 00:00)
    SELECT
        manager_id,
        client_created_at,
        manager_created_at,
        response_time_minutes,
        CASE
            -- Если время отклика до 09:30, корректируем время отклика на 1 минуту
            WHEN EXTRACT(HOUR FROM to_timestamp(manager_created_at)) < 9 OR (EXTRACT(HOUR FROM to_timestamp(manager_created_at)) = 9 AND EXTRACT(MINUTE FROM to_timestamp(manager_created_at)) < 30)
            THEN 1
            ELSE response_time_minutes
        END AS adjusted_response_time_minutes
    FROM response_times
)
-- Рассчитываем среднее время отклика для каждого менеджера
SELECT
    m.name_mop AS manager_name,
    AVG(adjusted_response_time_minutes) AS avg_response_time_minutes
FROM adjusted_response_times r
JOIN test.managers m ON r.manager_id = m.mop_id
GROUP BY manager_name
ORDER BY avg_response_time_minutes desc