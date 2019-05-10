library(sf)
library(ggplot2)
library(doParallel)

#The goal here is to iterate through the well pads and determine
#the areas of blocks that are at risk of contamination and to weight
#estimated well use into those areas so we can come up with a total
#number of wells at risk from each well pad

#First, lets import the well pad and the block data and then project them to
#a projection that's good for our working area.
pads <- st_read("/pine/scr/a/r/armurray/files/FracFocusSites.shp")
blocks <- st_read("/pine/scr/a/r/armurray/files/USBlocks_1km_FracFocus_NAD83.shp")
# Project data
pads <- st_transform(pads,3158)
blocks <- st_transform(blocks, 3158)

# You can subset the data here to troubleshoot without running the entire dataset
# pads <- pads[1:10,]


# In the following loop, we will iterate through every oil/gas well to create buffers and intersections with
# the census blocks within the buffer area. The reason we need to iterate through one gas well at a time is
# because if another gas well exists within the buffer distance, they will overlap and conflict
# with eachothers geometries, giving us bad results.

# Determine available cores and how many to use
acores <- detectCores()
ucores <- acores-10

# Tell doParallel how many cores to use
registerDoParallel(cores = ucores)


# Mark the start time
sTime <- Sys.time()
outdf <- data.frame()
pLoop <- foreach(k= 1:nrow(pads),.combine=rbind,.packages=c("dplyr","raster","sf","tidyr")) %dopar% {
  row <- pads[k,]
  buffer <- st_buffer(row, 1000)
  intersects <- st_intersection(blocks,buffer)
  intersects$NewArea <- st_area(intersects)
  intersects$wells <- intersects$Hybd_Blk_W * (intersects$NewArea / intersects$Shape_Area)
  wells <- sum(intersects$wells)
  new <- data.frame("API_Number"=row$API,"Est_Wells"=wells)
  outdf <- rbind(outdf, new)
}

eTime <- Sys.time()

write.csv(pLoop, "/pine/scr/a/r/armurray/outputs/FrackingProx2Wells.csv")

writeLines(c(paste0("Start Time: ",sTime),paste0("End Time: ", eTime), paste0("Available Cores: ", acores),
             paste0("Cores used: ", ucores), paste0("Time: ", eTime - sTime), paste0("Iterations: ", nrow(pads))),
           con = "/pine/scr/a/r/armurray/outputs/Frack_Messages.txt")
