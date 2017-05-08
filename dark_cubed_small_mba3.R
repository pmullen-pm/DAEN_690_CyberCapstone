#install.packages("R.utils")
#install.packages("arulesViz")
library("arulesViz")
library("arules")
library("R.utils")
library("boot")

##########################################################################
m1 <- read.transactions("d3_community5.csv", sep=",") #first 50 files
summary(m1)
ids_true <- apriori(m1,parameter = list(supp=0.0001, conf=0.7, minlen=2),
                              appearance = list(rhs=c("lastScore_greater_5_true","lastScore_greater_5_false"),
                                                default="lhs"),control = list(verbose=F))
subrules1 <- head(sort(ids_true, by="lift")[1:10])
#subrules1 <- inspect(sort(ids_true, by="lift")[1:10])
plot(subrules1, method="graph", control=list(type="itemsets"))



ids_false <- apriori(m1,parameter = list(supp=0.01, conf=0.7, minlen=2),
                     appearance = list(rhs=c("lastScore_greater_5_true","lastScore_greater_5_false"),
                                       default="lhs"),control = list(verbose=F))
subrules2 <- head(sort(ids_false, by="lift")[1:10])
#subrules2 <- inspect(sort(ids_false, by="lift")[1:10])
plot(subrules2, method="graph", control=list(type="itemsets"))
#plot(subrules2, method="graph", control=list(type="itemsets"))


#inspect(sort(ids,by="lift")[1:length(ids)])
#############################################################################
itemFrequencyPlot(m1,support=.01)
#subsetting--looking for specific items in rules
#last_score_greater_5_false <- subset(ids, items %in% "lastScore_greater_5_false")
#last_score_greater_5_true <- subset(ids, items %in% "lastScore_greater_5_true")
#inspect(last_score_greater_5_false)
#inspect(last_score_greater_5_true)

#matrix-based visualizations
m1_matrix <- ids_true[quality(ids_true)$confidence > 0.8]
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
plot(ids, method="graph")
#graph of itemsets
plot(m1, method="graph", control=list(type="itemsets"))
#parallel coordinates plot
plot(m1, method="paracoord")
