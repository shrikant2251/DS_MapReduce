
public class Map implements IMapper{

	@Override
	public String map(String text) {
		// TODO Auto-generated method stub
		if(text.toLowerCase().contains("temp"))
			return text;
		else
		return null;
	}

}
