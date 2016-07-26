public String getSeqFile(String inputLocation) throws Exception{
            String outputPath="<output path>";
	    FileSystem fs = null;
	    SequenceFile.Writer writer;
	    fs = FileSystem.get(getConfiguration());	    
	    Path vecoutput =new Path(outputPath);
	    writer = new SequenceFile.Writer(fs, getConfiguration(), vecoutput, Text.class, VectorWritable.class);
	    VectorWritable vec = new VectorWritable();
	    try {
	        FileReader fr = new FileReader(inputLocation);
	        BufferedReader br = new BufferedReader(fr);
	        String s = null;
	        String key = "Key";
	        while((s=br.readLine())!=null){
	        	//File Separator could be '/t',',','|' etc.
	            String spl[] = s.split(getFileSperator());
                    Integer val = 0;
			 for(int i=1;k<spl.length;i++){
	            			           
		          	colvalues[val] = Double.parseDouble(spl[i]);
		                val++;
			}       
	          
	            }
	            NamedVector nmv = new NamedVector(new DenseVector(colvalues),key);
	            vec.set(nmv);
	            writer.append(new Text(nmv.getName()), vec);
                    writer.close();
	            

	        } catch (Exception e) {
	        	System.out.println("ERROR: "+e);
	        }
	    return outputPath; 
	}

//A method for configuration setup.
private Configuration getConfiguration(){
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		return conf;
	}
