rm(list = ls())
require(tmvtnorm)
library(utils)
library(DescTools)
library(Hmisc)

# generate samples 
# script generates 1 mln samples from the truncated multivariate 
# student-t distribution

#
#   GENERATING DATA
#

#shares of each asset
shares <- c(0.2, 0.2, 0.2, 0.15, 0.15, 0.1)

# values of sigma columns
k1 <- c(1,1,0,1,1,1)
k2 <- c(1,36,-1,-1,-3,-5)
k3 <- c(0,-1,4,2,2,0)
k4 <- c(1,-1,2,49,-5,-2)
k5 <- c(1,-3,2,-5,16,-2)
k6 <- c(1,-5,0,-2,-2,9)

# s- amount of samples
# n - amount of assets 
s = 1000000
n = 6

# values of input parameters :
sigma <- matrix( c(k1, k2, k3, k4, k5, k6), ncol=n)
mu <- c(0.004, 0.002, 0.003, 0.004, 0.003, 0.001)
lower <- c(-0.1, -0.1, -0.1, -0.1, -0.1, -0.1)
upper <- c(0.1, 0.1, 0.1, 0.1, 0.1, 0.1)

# generating values for all wallet and splitting into assets 
wallet <- rtmvt(n=s, mean=mu, sigma=sigma, df=4, lower=lower, upper=upper, algorithm="gibbs")

# calculating the weighted average for wallet's rate of return
wallet_temp <- matrix(nrow=s,ncol=n)
for(i in 1: s){
  wallet_temp[i,] = shares * wallet[i,]
                  
}
wallet_column <- apply(wallet_temp, 1, sum)

#creating final matrix of rates of returns for assets and wallet
final_wallet <- cbind(wallet, wallet_column) 

# save samples to .csv file
write.table(final_wallet,	"wallet.csv",	sep=",")



#
#   CALCULATING STATISTICS
#

wallet1 <- sort(wallet_column, decreasing = FALSE)
asset1 <- sort(final_wallet[,1], decreasing = FALSE)
asset2 <- sort(final_wallet[,2], decreasing = FALSE)
asset3 <- sort(final_wallet[,3], decreasing = FALSE)
asset4 <- sort(final_wallet[,4], decreasing = FALSE)
asset5 <- sort(final_wallet[,5], decreasing = FALSE)
asset6 <- sort(final_wallet[,6], decreasing = FALSE)

sorted_wallet <- matrix(c(asset1,asset2, asset3, asset4, asset5,asset6, wallet1), ncol=7)

# get smallest 10% of samples
# r - amount of samples
n = n + 1
#sort(final_wallet, decreasing = FALSE)
r = nrow(sorted_wallet) /10
wallet_10_prc = matrix(nrow=r, ncol=n)
for(i in 1:r)
{
  wallet_10_prc[i,] = sorted_wallet[i,]
  print(i)
  i = i + 1
}

# calculating statistics for assets
means = matrix(ncol=n)
medians = matrix(ncol=n)
quantiles_10 = matrix(ncol=n)
means_10_prc = matrix(ncol=n)
securities_deviation = matrix(ncol=n)
securities_gini = matrix(ncol=n)
sumator = matrix(ncol=n)
sumator = c(0,0,0,0,0,0,0)
for(i in 1:n)
{
  for(x in 1:s)
  {
    sumator[i] = sumator[i] + i * sorted_wallet[s,i]
  }
  means[i] = mean(sorted_wallet[,i])
  medians[i] = median(sorted_wallet[,i])
  quantiles_10[i] = quantile(sorted_wallet[,i], probs =1/10)
  means_10_prc[i] = mean(wallet_10_prc[,i])
  securities_deviation[i] = (means[i] - 1/2 * MeanAD(sorted_wallet[,i]))
  securities_gini[i] = GiniMd(sorted_wallet[,i], na.rm=FALSE)
  print(i)
  i = i + 1
}



