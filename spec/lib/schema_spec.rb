
require '.\lib\schema'

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
			@schema = Schema.new('cube', 'event_ts', ['duration','count'], ['Code', 'Category', 'Detail'])
			@schema.dump
		end

		before(:each) do

		  	@factory_hash = {:timestamp => '2012-05-20', :attributes => {:Category => 'Business', :Code => '045', :Detail => 'random'},\
				:facts => {:count => 1, :duration => 20}}

			@where_id = factory_hash[:timestamp].gsub(/-/,'')	
			@test_cf = @schema.column_families.last.name
		end
		
		it "should correctly populate row_key" do
			@schema.insert(@factory_hash)

			result = Schema.db.execute("SELECT * from #{@test_cf} WHERE id =  '#{@where_id}'")
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

	it "should match correct attributes and facts combinations" do
			@factory_hash = {:attributes => {:Category => 'Business', :Code => '045', :Detail => 'random'},\
				:facts => {:count => 1, :duration => 20}}
	  		@ccf.match(@factory_hash).should be(true)
	end


end