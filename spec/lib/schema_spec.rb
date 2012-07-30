
require './lib/schema'

describe Schema do

	before(:each) do
		@schema = Schema.new('cube', 'event_ts', ['duration','count'], ['Code', 'Category', 'Detail'])
	end

	describe "#initialize" do

		it "names with whitespaces should throw exception" do
		  expect {Schema.new('Name with whitespaces', 'event_ts')}.to raise_error
		end

		it "timestamps with whitespaces should throw exception" do
		  expect {Schema.new('cube', 'whitespace ts')}.to raise_error
		end

		it "should have sorted attributes array" do
			@schema.attributes.should satisfy {|x,y| x <= y}
		end

		it "should have sorted facts array" do
			@schema.facts.should satisfy {|x,y| x <= y}
		end

		it "should have timestamp column defined" do
			@schema.timestamp.should be_true
		end

		it "should populate correct number of cube_column_families" do
		  @schema.column_families.should have(4*2).items
		end

		it "should populate cube_column_families with correct names" do
		  @schema.column_families.map {|x| x.name}.should include ('Category__Code__Detail__count')
		end

	end

	describe "#dump" do


		it "should not try to create keyspace if it's already there" do
			ks = double(:name => @schema.name)
			Schema.db.should_receive(:keyspaces).and_return([ks])

			@schema.dump.should be_false
		end

		it "should create column families in cassandra" do
			@schema.dump
			ks = Schema.db.keyspaces.select {|k| k.name == @schema.name}
			ks[0].column_family_names.count.should eql(8)
			
			# teardown
			@schema.whipeout!
		end
	end

	
	describe "#combinations" do

		it "should return exactly 1 attributes combination if no sub-totals are required" do
			@schema.attributes_combination.should have(1).item
		end

		it "should return SUM(C(n,k)) attributes combination if sub-total are required" do
		  	@schema.attributes_combination(2).should have(4).items
		end

		it "should return sotred array (by 2 directions)" do
		  @schema.attributes_combination(1).should eq([['Category'],['Code'], ['Detail'],['Category','Code'], \
		  	['Category','Detail'],['Code','Detail'],['Category','Code','Detail']])
		end
	end

end

describe Schema, "#insert" do

		before(:all) do
			@schema = Schema.new('cube', 'event_ts', ['duration','caunt'], ['Code', 'Category', 'Detail'])
			@schema.dump
		end

		after(:all) do
			@schema.whipeout!
		end

		before(:each) do
			@timestamp = '2012-05-20'
			@attributes = {:Category => 'Business', :Code => '045', :Detail => 'random'}
			@facts = {:count => 1, :duration => 20}

			@record = CubedRecord.new(@timestamp, @attributes, @facts)	

			@where_id = @timestamp.gsub(/-/,'')	
			@test_cf = @schema.column_families.last.name
		end
		
		it "should correctly populate row_key" do
			@schema.insert(@record)
			puts "#{@test_cf}"
			result = Schema.db.execute("SELECT * from Category__Code__Detail__duration WHERE id =  '20120520'")
			result.rows.should eq(1)
		end

	end

describe CubeColumnFamily do

	before(:each) do
	  @ccf = CubeColumnFamily.new(['Category','Code','Detail'], 'count')

	end

	it "name should be correctly populated" do
		@ccf.name.should eql('Category__Code__Detail__count')
	end

	it "insert string should be correctly populated" 

end

describe CubedRecord do

	before(:each) do
		@timestamp = '2012-05-20'
		@attributes = {:Category => 'Business', :Code => '045', :Detail => 'random'}
		@facts = {:count => 1, :duration => 20}

		@record = CubedRecord.new(@timestamp, @attributes, @facts)		

		@cf = CubeColumnFamily.new(['Category','Code','Detail'], 'count')
	end

	it "should be projected to appropriate Column Family" do
		@record.project(@cf).should be_true
	end

	it "should return correct projection result with appropriate Column Family" do
		@result_record = CubedRecord.new(@timestamp, @attributes, {:count => 1})

		@record.project(@cf).should == @result_record
	end

	it "should return correct projection result in case of sub-totals with appropriate Column Family" do
		@cf = CubeColumnFamily.new(['Category','Code'], 'count')

		@result_record = CubedRecord.new(@timestamp, {:Category => 'Business', :Code => '045'}, {:count => 1})

		@record.project(@cf).should == @result_record
	end

	it "should return correct projection result in case of broad record with appropriate Column Family" do
		@record = CubedRecord.new(@timestamp, @attributes.merge({:New => 'new'}), @facts.merge({:new => 1}))
		@result_record = CubedRecord.new(@timestamp, @attributes, {:count => 1})

		@record.project(@cf).should == @result_record
	end

	it "should return false if Column Family has inappropriate attribute" do
		@cf = CubeColumnFamily.new(['Category','Code','Error'], 'count')

		@record.project(@cf).should be_false
	end

	it "should return false if Column Family has inappropriate fact" do
		@cf = CubeColumnFamily.new(['Category','Code','Detail'], 'error')

		@record.project(@cf).should be_false
	end
end