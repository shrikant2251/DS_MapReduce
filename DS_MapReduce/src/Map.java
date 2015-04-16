
public class Map implements IMapper{

	@Override
	public String map(String text,String searchKey) {
		// TODO Auto-generated method stub
		System.out.println("Map Class map Method line:" + text);
		if(text.toLowerCase().contains(searchKey))
			return text;
		else
		return null;
	}

}
