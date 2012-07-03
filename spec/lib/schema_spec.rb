
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

describe CubeColumnFamily do

	before(:each) do
	  @ccf = CubeColumnFamily.new(['Category','Code','Detail'], 'count')
	end

	it "name should be correctly populated" do
		@ccf.name.should eql('Category__Code__Detail__count')
	end
end