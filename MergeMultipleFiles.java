
    /**
     * Merge multiple memory mapped files in a single go.
     * Send the last File's size separately.
     * fileChannelArray - list of memery mapped files to be merged in ascending order
     */
    private static void mergeAllFiles(List<MappedByteBuffer> fileChannelsArray, long commonFileSize, long sizeOfLastFile) {
        int noOfFilesToBeMerged = fileChannelsArray.size();
        long[] entriesInFile = new long[noOfFilesToBeMerged];

        Arrays.fill(entriesInFile, commonFileSize);
        entriesInFile[noOfFilesToBeMerged - 1] = sizeOfLastFile;

        long[] filePositions = new long[noOfFilesToBeMerged];
        Arrays.fill(filePositions, 0L);

        int mergedFiles = 0;
        int minValueFileIndex = 0;
        ArrayList<Integer> startIndexHelper = new ArrayList<Integer>();
        long[] currentValueFromFiles = new long[noOfFilesToBeMerged];
        boolean[] merged = new boolean[noOfFilesToBeMerged];

        for (int i = 0; i < noOfFilesToBeMerged -1 ; ++i) {
                filePositions[i] += 1;
                startIndexHelper.add(i);
                currentValueFromFiles[i] = fileChannelsArray.get(i).getLong();
        }
        long total = 0;
        while (mergedFiles < noOfFilesToBeMerged) {

            Long minValue = null;
            int startIndex ;
            for (startIndex = 0; startIndex < noOfFilesToBeMerged; ++startIndex)  {
                if (!merged[startIndex]) {
                    minValue = currentValueFromFiles[startIndex];
                    minValueFileIndex = startIndex;
                    break;
                }
            };

            long min = minValue;
            for (int i = startIndex + 1; i < noOfFilesToBeMerged; ++i ) {
               if (!merged[i] && min > currentValueFromFiles[i]) {
                   min = currentValueFromFiles[i];
                   minValueFileIndex = i;
               }
            }

            if (filePositions[minValueFileIndex] < entriesInFile[minValueFileIndex]) {
                filePositions[minValueFileIndex] += 1;
                currentValueFromFiles[minValueFileIndex] = fileChannelsArray.get(minValueFileIndex).getLong();
            } else {
                // content of this file is completely merged. Ignore this file from futher merging.
                //System.out.println(" current count :" + filePositions[minValueFileIndex]     + " total count in file :" + entriesInFile[minValueFileIndex]);
                merged[minValueFileIndex] = true;
                ++mergedFiles;
            }

            ++total;
        }
        System.out.println(" sorted " + total + " entries");//use this to reconcile the count of records
    }
