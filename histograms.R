rm(list = ls())

#Odczyt danych z plików

input <- read.csv(file = 'alerts3.csv', header = FALSE)

#Tworzenie tymczasowych zmiennych i tablic

wallet_V3 = input$V3[which(input$V2 == "wallet")]

wallet_V4 = input$V4[which(input$V2 == "wallet")]

wallet_s <- data.frame(wallet_V3,wallet_V4)



asset1_V3 = input$V3[which(input$V2 == "asset1")]

asset1_V4 = input$V4[which(input$V2 == "asset1")]

asset1_s <- data.frame(asset1_V3,asset1_V4)



asset2_V3 = input$V3[which(input$V2 == "asset2")]

asset2_V4 = input$V4[which(input$V2 == "asset2")]

asset2_s <- data.frame(asset2_V3,asset2_V4)



asset3_V3 = input$V3[which(input$V2 == "asset3")]

asset3_V4 = input$V4[which(input$V2 == "asset3")]

asset3_s <- data.frame(asset3_V3,asset3_V4)



asset4_V3 = input$V3[which(input$V2 == "asset4")]

asset4_V4 = input$V4[which(input$V2 == "asset4")]

asset4_s <- data.frame(asset4_V3,asset4_V4)



asset5_V3 = input$V3[which(input$V2 == "asset5")]

asset5_V4 = input$V4[which(input$V2 == "asset5")]

asset5_s <- data.frame(asset5_V3,asset5_V4)



asset6_V3 = input$V3[which(input$V2 == "asset6")]

asset6_V4 = input$V4[which(input$V2 == "asset6")]

asset6_s <- data.frame(asset6_V3,asset6_V4)



#Tworzenie zmiennych tablicowych zawieraj¹ce przefiltrowane dane

wallet_average = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "average")]

wallet_average_min_10 = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "average min 10%")]

wallet_median = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "median")]

wallet_security_deviation = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "security deviation")]

wallet_security_gini = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "security gini")]

wallet_quantile_10 = wallet_s$wallet_V4[which(wallet_s$wallet_V3 == "quantile 10%")]

rm(wallet_V3,wallet_V4,wallet_s)



asset1_average = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "average")]

asset1_average_min_10 = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "average min 10%")]

asset1_median = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "median")]

asset1_security_deviation = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "security deviation")]

asset1_security_gini = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "security gini")]

asset1_quantile_10 = asset1_s$asset1_V4[which(asset1_s$asset1_V3 == "quantile 10%")]

rm(asset1_V3,asset1_V4,asset1_s)



asset2_average = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "average")]

asset2_average_min_10 = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "average min 10%")]

asset2_median = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "median")]

asset2_security_deviation = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "security deviation")]

asset2_security_gini = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "security gini")]

asset2_quantile_10 = asset2_s$asset2_V4[which(asset2_s$asset2_V3 == "quantile 10%")]

rm(asset2_V3,asset2_V4,asset2_s)



asset3_average = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "average")]

asset3_average_min_10 = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "average min 10%")]

asset3_median = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "median")]

asset3_security_deviation = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "security deviation")]

asset3_security_gini = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "security gini")]

asset3_quantile_10 = asset3_s$asset3_V4[which(asset3_s$asset3_V3 == "quantile 10%")]

rm(asset3_V3,asset3_V4,asset3_s)



asset4_average = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "average")]

asset4_average_min_10 = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "average min 10%")]

asset4_median = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "median")]

asset4_security_deviation = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "security deviation")]

asset4_security_gini = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "security gini")]

asset4_quantile_10 = asset4_s$asset4_V4[which(asset4_s$asset4_V3 == "quantile 10%")]

rm(asset4_V3,asset4_V4,asset4_s)



asset5_average = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "average")]

asset5_average_min_10 = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "average min 10%")]

asset5_median = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "median")]

asset5_security_deviation = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "security deviation")]

asset5_security_gini = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "security gini")]

asset5_quantile_10 = asset5_s$asset5_V4[which(asset5_s$asset5_V3 == "quantile 10%")]

rm(asset5_V3,asset5_V4,asset5_s)



asset6_average = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "average")]

asset6_average_min_10 = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "average min 10%")]

asset6_median = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "median")]

asset6_security_deviation = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "security deviation")]

asset6_security_gini = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "security gini")]

asset6_quantile_10 = asset6_s$asset6_V4[which(asset6_s$asset6_V3 == "quantile 10%")]

rm(asset6_V3,asset6_V4,asset6_s)



#############################################################################################################

hist(asset1_average)
hist(asset2_average)
hist(asset3_average)
hist(asset4_average)
hist(asset5_average)
hist(asset6_average)
hist(wallet_average)

hist(asset1_median)
hist(asset2_median)
hist(asset3_median)
hist(asset4_median)
hist(asset6_median)
hist(asset6_median)
hist(wallet_median)

hist(asset1_quantile_10)
hist(asset2_quantile_10)
hist(asset3_quantile_10)
hist(asset4_quantile_10)
hist(asset6_quantile_10)
hist(asset6_quantile_10)
hist(wallet_quantile_10)

hist(asset1_average_min_10)
hist(asset2_average_min_10)
hist(asset3_average_min_10)
hist(asset4_average_min_10)
hist(asset6_average_min_10)
hist(asset6_average_min_10)
hist(wallet_average_min_10)

hist(asset1_security_deviation)
hist(asset2_security_deviation)
hist(asset3_security_deviation)
hist(asset4_security_deviation)
hist(asset5_security_deviation)
hist(asset6_security_deviation)
hist(wallet_security_deviation)

hist(asset1_security_gini)
hist(asset2_security_gini)
hist(asset3_security_gini)
hist(asset4_security_gini)
hist(asset5_security_gini)
hist(asset6_security_gini)
hist(wallet_security_gini)