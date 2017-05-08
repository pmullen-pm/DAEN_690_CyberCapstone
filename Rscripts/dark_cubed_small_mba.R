#install.packages("R.utils")
#install.packages("arulesViz")
library("arulesViz")
library("arules")
library("R.utils")
library("boot")
#nL <- countLines("train_ver2.csv");nL

df <- read.csv("dark_transformed.csv",header = T)
head(df)
#View(df)

#association rules/market basket analysis
require(arules)
dark <- read.transactions("dark_transformed.csv",sep=",")
summary(dark)

#now we go a little deeper
inspect(dark[1:3])#inspect first 3 transactions
#look at the support
itemFrequency(dark[,1])

#now look at first 6 items alphabetically
itemFrequency(dark[,1:6])
#now look at a plot
itemFrequencyPlot(dark,support=.01)
#show top 5 items
itemFrequencyPlot(dark, topN=5)
itemFrequencyPlot(dark, topN=17)

m2 <- apriori(dark,parameter = list(minlen=2, supp=0.000005, conf=0.6),
              appearance = list(rhs=c("low_threat_1","low_threat_2",
              "low_threat_3", "mid_threat_4","mid_threat_6", 
              "high_threat_7", "high_threat_8"),default="lhs"),
              control = list(verbose=F))
inspect(sort(m2, by="lift"))






#find confidence
m1 <- apriori(dark)#this yields no rules b/c the default minimums too stringent (.1 for support, .8 for confidence)
m1 <- apriori(dark,parameter=list(support=0.10,confidence=0.80,minlen=2))
m1
summary(m1)
inspect(m1[1:2])#look at first 2

#lift is how much more likely an item is to be purchased
#with another item than by itself
#let's sort the rules
inspect(sort(m1,by="lift")[1:4])#top 4 rules by lift
inspect(sort(m1,by="lift")[4:10])#4 thru 10 rules by lift
inspect(sort(m1,by="lift")[1:10])
inspect(sort(m1,by="lift")[1:length(m1)])
################################################################################

dev.off()

#visualizations
plot(m1)#scatterplot

#interactive
plot(m1, measure=c("support", "lift"), shading="confidence", interactive=TRUE)

#matrix-based visualizations
m1_matrix <- m1[quality(m1)$confidence > 0.8]

#plot of matrix visualization
plot(m1_matrix, method="matrix", measure="lift")

#reorder rows and columns such that rules with similar values of interest measure are presented closer together
plot(m1_matrix, method="matrix", measure="lift", control=list(reorder=TRUE))

#use 3d bars instead of rectangles
plot(m1_matrix, method="matrix3D", measure="lift")

#reorder rows and columns such that rules with similar values of interest measure are presented closer together
plot(m1_matrix, method="matrix3D", measure="lift", control=list(reorder=TRUE))

#use color hue
plot(m1_matrix, method="matrix", measure=c("lift", "confidence"))

#reorder again
plot(m1_matrix, method="matrix", measure=c("lift", "confidence"),
     control=list(reorder=TRUE))
#grouped rules
plot(m1_matrix, method="grouped") #all of them

#grouped, reordered
plot(m1_matrix, method="grouped", control=list(k=5)) #only 5 rules

#interactive version of grouped, reordered
plot(m1_matrix, method="grouped", interactive=TRUE)

#graph based visualization
plot(m1, method="graph")


#graph of itemsets
plot(m1, method="graph", control=list(type="itemsets"))

#parallel coordinates plot
plot(m1, method="paracoord")

###################################################################
log_data <- prod; head(log_data,1)
attach(log_data)
regression <- df; head(regression)

#logistic regression
lm.fit1 = glm(regression$V21~1+V25+V26,data=regression,family=binomial); lm.fit1$aic 


# model diagnostics
cv_res = cv.glm(log_data, lm.fit1, K=10)
cv_res$delta[1] #
lm.fit1$aic #



#need training data here
pred = predict(lm.fit1,newdata = d1)
data.frame(d1$ViolentCrimesPerPop,pred)[1:20,]
R2(d1$ViolentCrimesPerPop,pred,family = "gaussian")#0.7005332
plot(lm.fit1)