
public class Map1 implements IMapper{

	@Override
	public String map(String text) {
		// TODO Auto-generated method stub
		if(text.toLowerCase().contains("pwd"))
			return text;
		else
		return null;
	}

}
