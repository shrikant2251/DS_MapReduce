
public class Map1 implements IMapper{

	@Override
	public String map(String text,String searchKey) {
		// TODO Auto-generated method stub
		System.out.print("Search Key ^^^:"+ searchKey+"^^^ Map Class map Method line:" + text + "------->");
		if(text.toLowerCase().contains(searchKey.toLowerCase())){
			System.out.println("Text Found!!!!!!!!!!!!!!!!!!!!");
			return text;
		}
		else{
			System.out.println("Text Not Found!!!!!!!!!!!!!!!!!!!!");
			return null;
		}
	}

}
