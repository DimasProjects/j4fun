with tempTable as (
  select userId, category, ses_start as sessionStartTime, ses_max as sessionEndTime
    from(
      select userId, category, ses_num, min(eventTime) as ses_start, max(eventTime) as ses_max
        from(
        select userId, category, eventTime,
          sum(changed) over(partition by userId order by eventTime rows between unbounded preceding and current row) as ses_num
          from (
          select userId, category, eventTime,
            case when lag(category) over(partition by userId order by eventTime) = category then 0 else 1 end as changed
            from InitData
        )
      )
      group by userId, category, ses_num
  )
)

select id.userId, id.category,
       id.product, id.eventType,
       id.eventTime, tt.sessionStartTime,
       tt.sessionEndTime, concat(id.userId, '_', id.category) as sessionId
from InitData id
  inner join tempTable tt
  on tt.userId = id.userId
  where id.category = tt.category
  and id.eventTime between tt.sessionStartTime and sessionEndTime


--For each category find median session duration
select category, percentile_approx(delta, 0.5) as median from (
  select category, (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) as delta from (
    select category,
      min(sessionStartTime) over(partition by category, sessionId order by sessionStartTime) as minInGroup,
      max(sessionEndTime) over(partition by category, sessionId order by sessionEndTime) as maxInGroup
      from Analysis
  )
) group by category

--For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
select category, description, count(userId)
from (
  select category, userId,
  case when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) < 60 then 'less then 1 min'
       when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) between 60 and 300 then '1 to 5 mins'
       when (unix_timestamp(maxInGroup) - unix_timestamp(minInGroup)) > 300 then 'more than 5 mins'
  END
  as description
  from(
    select category, userId,
      min(sessionStartTime) over(partition by category, userId order by sessionStartTime) as minInGroup,
      max(sessionEndTime) over(partition by category, userId order by sessionEndTime) as maxInGroup
      from Analysis
  )
) group by category, description

--For each category find top 10 products ranked by time spent by users on product pages
