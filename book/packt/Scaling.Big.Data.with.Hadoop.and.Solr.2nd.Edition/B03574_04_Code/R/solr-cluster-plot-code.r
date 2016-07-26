###################################### Cluster Plot on Apache Solr ##############
library(cluster)
library(solr)
url <- 'http://localhost:8983/solr/select'
response1 <- solr_group(q='*:Solr', group.field='Country', rows=10, group.limit=1, base=url)
m2 <- matrix(response1$numFound,byrow=TRUE)
rownames(m2) <- response1$groupValue
colnames(m2) <- 'Available Workforce';
fit <- kmeans(m2, 2)
clusplot(m2, fit$cluster, color=TRUE, shade=TRUE,labels=2, lines=0, xlab="Workforce", ylab="Cluster", main="K-Means Cluster")
######################################
