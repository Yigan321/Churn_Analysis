Problem Statement: Can we predict customer churn based on features like life time value most recent order date etc

I did some research on customer churn and came across two topics that deal with contractural and non contractual data. Meaning, Dispatch customers are not in a contractural setting ( I dont think so) if they were I would utilize the Kaplan Meier model because it works best with contractural data. (Because the customers are in contract I can easily tell when they churn without calculating). However, I would have to use the derivative of the Pareto/NBD Model called BG/NBD model that focuses on non contractural customer setting. From my understanding the model needs only 3 factors. Frequency Recency and Monetary
-The number of transactions a customer has made after their first transaction. -The time of a customer's last purchase. -How long the customer been tracked for.
The BG/NBD model quantifies the lifetime value of a customer by assessing the expected number of transactions the customer will have in the future as long as they has not churned. There are two options avaliable for the customer. First the customer purchases or does not purchase again, and Second the customer churns or does not churn. These two options are probabilisitic, meaning that there is a probability associated with the customer pruchasing at a given period and a different probability of a customer churning after a purchase.

The model assumes the following:

Until a customer churns, the number of transactions made by a customer follows a Poisson process - a simple and widely used stochastic process for modeling the times at which arrivals enter a system.
So at every period (week) in a defined time interval (one year = 52 weeks), the customer decides whether to pruchase or not. The number of purchases a customer makes is dependent on each customer's probability distribution around lambda, the arrival rate.
Goals:

LTV = expected number of transactions * revenue per transactions * margin
BG/NBD model: predict 1) expected number of transactions; 2) probability of being alive Gamma-Gamma model: predict expected monetary value per transaction Margin is defined by business, e.g., discount, campaign, something relates to the price
