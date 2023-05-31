with customer_reference_1 as
              (select
              dispatch_orders.organization_id,
              dispatch_orders.organization_name,
              dispatch_orders.needed_vehicle_type,
              date_trunc('week',delivery_completed_at) as week_date,
              count(case when dispatch_orders.needed_vehicle_type = 'box_truck' then dispatch_orders.id else null end) as box_truck_delivered_orders
              from prod_analytics.dispatch_orders
              left outer join prod_analytics.dispatch_drivers on dispatch_drivers.driver_id = dispatch_orders.driver_id
              where dispatch_orders.status = 'delivered' and dispatch_drivers.vehicle_type = 'box_truck'
              group by 1,2,3,4
              order by 4 desc, 5 desc),

      customer_reference_2 as
      (select
      customer_reference_1.organization_id,
      customer_reference_1.week_date as current_week,
      customer_reference_1.week_date - interval '1 week' as previous_week,
      customer_reference_1.box_truck_delivered_orders as box_truck_delivered_orders_current,
      customer_reference_1a.box_truck_delivered_orders as box_truck_delivered_orders_previous
      from customer_reference_1
      left outer join customer_reference_1 as customer_reference_1a on customer_reference_1a.organization_id = customer_reference_1.organization_id and customer_reference_1.week_date = customer_reference_1a.week_date + interval '1 week'
      ),
      distinct_weeks as
      (
      select
      distinct first_day_of_week as date_week,
      date_week - interval '1 week' as previous_week
      from prod_analytics_helpers.date_details
      )
      select
      distinct_weeks.date_week as current_week_date,
      count(case when current_week_date = customer_reference_2.current_week and customer_reference_2.box_truck_delivered_orders_current > 0 then organization_id else null end) as current_week_bt_active_org_count,
      count(case when current_week_date - interval '1 week' = customer_reference_2.current_week and customer_reference_2.box_truck_delivered_orders_current > 0 then organization_id else null end) as previous_week_bt_active_org_count,
      count(case when current_week_date = customer_reference_2.current_week and customer_reference_2.box_truck_delivered_orders_current > 0 and current_week_date - interval '1 week' = customer_reference_2.previous_week and customer_reference_2.box_truck_delivered_orders_previous > 0 then organization_id else null end) as both_weeks_bt_active_org_count,
      case when previous_week_bt_active_org_count = 0 then 0 else 1 - (both_weeks_bt_active_org_count::numeric / previous_week_bt_active_org_count::numeric) end as churn_percentage
      from customer_reference_2
      left outer join distinct_weeks on distinct_weeks.date_week = distinct_weeks.date_week
      where current_week_date < current_date
      group by 1
      order by 1 desc
