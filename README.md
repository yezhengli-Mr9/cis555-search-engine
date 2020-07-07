This project builds a functional web search engine that incorporates what we have learned in CIS555. To achieve this aim, we have the following goals in mind: a) Our system is able to crawl a large corpus of web documents. (4*10^5 urls,200GB AWS S3) b) Our system can process large amounts of data efficiently. c) Our system can return accurate and meaningful results based on user search query. d) Our system is robust and can be deployed on the cloud as a real product.

--------
While details of scalability (of the project) can be found in our [final report](report/CIS555FinalProjectReport.pdf), 

(1) we crawled ```4*10^5``` urls with 200GB data stored on AWS S3.

(2) indexer is on AWS RDS mysql wih 35-70GB invertedIndex (as well as tf-idf scores, summary statistics etc.).

--------

Full name:  Feng Xiang, Yezheng Li, Xinyu Ma, Shenqi Hu

SEAS login: fxiang, yezheng, xinyuma, hshenqi

Which features did you implement? 

  (list features, or write 'Entire assignment')
  
  Entire assignment
  
Did you complete any extra-credit tasks? If so, which ones?

  (list extra-credit tasks)
  
  Process pagerank using Apache Spark

Any special instructions for building and running your solution?

  (include detailed instructions, or write 'None')
  
  None

Did you personally write _all_ the code you are submitting
(other than code from the course web page)?

  [x] Yes
  
  [ ] No

Did you copy any code from the Internet, or from classmates?

  [ ] Yes
  
  [x] No

Did you collaborate with anyone on this assignment?

  [ ] Yes
  
  [x] No


-----

One of the screenshots (all saved screenshots are in [report](report/) as well) is shown as following
![picture](report/G18-saudi-aramco.png)
